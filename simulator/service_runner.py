"""The core simulation loop — one asyncio Task per service."""

import asyncio
import random
import time

from shared.models import Event, EventType, CircuitState, ServiceState
from shared.event_bus import EventBus
from shared.state_store import StateStore
from shared.config import SERVICE_CONFIGS, DEPENDENCY_GRAPH

from simulator.latency_model import (
    compute_latency,
    compute_failure_probability,
    compute_cpu_usage,
    exponential_moving_average,
)
from simulator.circuit_breaker import CircuitBreakerRegistry
from simulator.failure_engine import CascadeEngine
from simulator.retry_storm import storm_detector, compute_backoff, BackoffConfig



MAX_RETRIES = 3
BASE_RETRY_DELAY_S = 0.1
RETRY_JITTER_S = 0.05


retry_storm_detector = storm_detector
_backoff_config = BackoffConfig(
    base_delay_s  = 0.1,
    max_delay_s   = 5.0,
    multiplier    = 2.0,
    jitter_factor = 0.25,
    max_retries   = MAX_RETRIES,
)



async def run_service(
    service_name: str,
    bus:          EventBus,
    store:        StateStore,
    cb_registry:  CircuitBreakerRegistry,
    cascade:      CascadeEngine,
    event_log:    list,
) -> None:
    config   = SERVICE_CONFIGS[service_name]
    cb       = cb_registry.get(service_name)
    ema_a    = 0.1   # EMA smoothing factor for latency and error_rate

    cascade_failure_boost: float = 0.0
    cascade_latency_boost: float = 0.0

    while True:
        event = await bus.consume(service_name)

        state = store.get_sync(service_name)
        if state is None:
            continue

        if not state.is_alive:
            await store.increment(service_name, "total_errors")
            _log(event_log, f"DEAD   {service_name:<14} trace={event.trace_id[:8]}", "red")
            continue

        allowed = await cb.allow_request()
        if not allowed:
            await store.increment(service_name, "total_errors")
            _log(
                event_log,
                f"BLOCKED  {service_name:<14} circuit={cb.state.value}  "
                f"trace={event.trace_id[:8]}",
                "red",
            )
            continue

        await store.increment(service_name, "active_requests")
        await store.increment(service_name, "total_requests")

        if event.event_type != EventType.RETRY:
            storm_detector.record_request(service_name)

        new_depth = bus.queue_depth(service_name)
        await store.update(service_name, queue_depth=new_depth)

        state = store.get_sync(service_name)
        new_cpu = compute_cpu_usage(state, replicas=state.replicas)
        await store.update(service_name, cpu_usage=new_cpu)

        state    = store.get_sync(service_name)
        latency  = compute_latency(config, state)

        latency += cascade_latency_boost

        latency += state.extra_latency_ms

        await asyncio.sleep(latency / 1000.0)

        state = store.get_sync(service_name)

        fail_prob = compute_failure_probability(config, state)
        fail_prob = min(fail_prob + cascade_failure_boost, 0.95)

        failed = random.random() < fail_prob

        new_active = max(0, state.active_requests - 1)
        await store.update(service_name, active_requests=new_active)

        if failed:
            await store.increment(service_name, "total_errors")

            state         = store.get_sync(service_name)
            new_error_rate = exponential_moving_average(state.error_rate, 1.0, ema_a)
            new_avg_lat    = exponential_moving_average(state.avg_latency, latency, ema_a)

            _log(
                event_log,
                f"FAIL   {service_name:<14} trace={event.trace_id[:8]}  "
                f"lat={latency:>6.1f}ms  err={new_error_rate:.0%}  "
                f"retry={event.retry_count}",
                "red",
            )

            await cb.record_failure()

            await store.update(service_name,
                error_rate    = new_error_rate,
                avg_latency   = new_avg_lat,
                circuit_state = cb.state,
            )

            if event.retry_count < MAX_RETRIES:
                backoff = compute_backoff(event.retry_count, _backoff_config)

                await asyncio.sleep(backoff)

                retry_event = Event(
                    source_service = event.source_service,
                    target_service = service_name,
                    event_type     = EventType.RETRY,
                    trace_id       = event.trace_id,
                    retry_count    = event.retry_count + 1,
                    payload_size   = event.payload_size,
                )

                accepted = await bus.publish(service_name, retry_event)
                if accepted:
                    storm_detector.record_retry(service_name)
                    is_storm = storm_detector.is_storming(service_name)
                    amp      = storm_detector.amplification_factor(service_name)

                    storm_tag = f" ⚡STORM amp={amp:.1f}x" if is_storm else ""
                    _log(
                        event_log,
                        f"RETRY  {service_name:<14} trace={event.trace_id[:8]}  "
                        f"attempt={event.retry_count+1}/{MAX_RETRIES}  "
                        f"backoff={backoff:.2f}s{storm_tag}",
                        "red" if is_storm else "yellow",
                    )
                else:
                    _log(
                        event_log,
                        f"RETRY DROPPED  {service_name}  queue full",
                        "red",
                    )

            state    = store.get_sync(service_name)
            cascades = cascade.evaluate(service_name, state)

            for impact in cascades:
                upstream = impact["service"]
                upstream_state = store.get_sync(upstream)
                if upstream_state:
                    _cascade_impacts[upstream] = {
                        "failure_boost": impact["failure_impact"],
                        "latency_boost": impact["latency_impact"],
                    }
                    if impact["severity"] in ("medium", "high"):
                        _log(
                            event_log,
                            f"CASCADE  {service_name}→{upstream}  "
                            f"impact={impact['failure_impact']:.0%} failure  "
                            f"+{impact['latency_impact']:.0f}ms latency",
                            "magenta",
                        )

        else:
            state         = store.get_sync(service_name)
            new_error_rate = exponential_moving_average(state.error_rate, 0.0, ema_a)
            new_avg_lat    = exponential_moving_average(state.avg_latency, latency, ema_a)

            # Record success in circuit breaker window
            await cb.record_success()

            await store.update(service_name,
                error_rate    = new_error_rate,
                avg_latency   = new_avg_lat,
                circuit_state = cb.state,
            )

            if new_error_rate < 0.05:
                _cascade_impacts.pop(service_name, None)

            if random.random() < 0.15:
                _log(
                    event_log,
                    f"OK     {service_name:<14} trace={event.trace_id[:8]}  "
                    f"lat={latency:>6.1f}ms  err={new_error_rate:.0%}",
                    "green",
                )

            for downstream in DEPENDENCY_GRAPH.get(service_name, []):
                downstream_event = Event(
                    source_service = service_name,
                    target_service = downstream,
                    event_type     = EventType.REQUEST,
                    # Propagate the trace_id so the full chain is traceable
                    trace_id       = event.trace_id,
                    payload_size   = event.payload_size,
                )
                accepted = await bus.publish(downstream, downstream_event)
                if not accepted:
                        _log(
                        event_log,
                        f"BACKPRESSURE  {service_name}→{downstream}  "
                        f"queue full (depth={bus.queue_depth(downstream)})",
                        "magenta",
                    )

        my_impact = _cascade_impacts.get(service_name, {})
        cascade_failure_boost = my_impact.get("failure_boost", 0.0)
        cascade_latency_boost = my_impact.get("latency_boost", 0.0)


_cascade_impacts: dict[str, dict] = {}



def _log(event_log: list, message: str, color: str = "white") -> None:
    import time as _time
    event_log.append({
        "time":    _time.strftime("%H:%M:%S"),
        "message": message,
        "color":   color,
    })
    if len(event_log) > 200:
        event_log.pop(0)
