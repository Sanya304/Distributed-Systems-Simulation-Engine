"""Stage 2 tests for latency, circuit breaker, and cascade engine."""

import asyncio
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.models import ServiceConfig, ServiceState, CircuitState
from shared.config import SERVICE_CONFIGS
from simulator.latency_model import (
    compute_latency,
    compute_failure_probability,
    compute_cpu_usage,
    exponential_moving_average,
)
from simulator.circuit_breaker import CircuitBreaker, CircuitBreakerRegistry
from simulator.failure_engine import (
    CascadeEngine,
    build_reverse_graph,
    get_upstream_services,
    compute_cascade_impact,
)



async def test_latency_model():
    print("\n--- Test 1: Latency Model ---")

    config = SERVICE_CONFIGS["payment"]
    idle_state = ServiceState(
        name="payment",
        active_requests=0,
        queue_depth=0,
        cpu_usage=0.0,
    )
    idle_latency = compute_latency(config, idle_state)
    assert 40 < idle_latency < 160
    print(f"  Idle latency: {idle_latency:.1f}ms (base={config.base_latency_ms}ms)")

    half_queue_state = ServiceState(
        name="payment",
        active_requests=5,
        queue_depth=100,
        cpu_usage=0.3,
    )
    half_latency = compute_latency(config, half_queue_state)
    assert half_latency > idle_latency
    print(f"  50% queue latency: {half_latency:.1f}ms")

    overload_state = ServiceState(
        name="payment",
        active_requests=20,
        queue_depth=190,
        cpu_usage=0.95,
    )
    overload_latency = compute_latency(config, overload_state)
    assert overload_latency > half_latency
    assert overload_latency > 200
    print(f"  Overload latency: {overload_latency:.1f}ms")

    idle_fail = compute_failure_probability(config, idle_state)
    overload_fail = compute_failure_probability(config, overload_state)
    assert overload_fail > idle_fail
    print(f"  Failure rate idle: {idle_fail:.1%}")
    print(f"  Failure rate overload: {overload_fail:.1%}")

    low_cpu_state = ServiceState(name="payment", active_requests=5)
    high_cpu_state = ServiceState(name="payment", active_requests=28)
    low_cpu = compute_cpu_usage(low_cpu_state, replicas=3)
    high_cpu = compute_cpu_usage(high_cpu_state, replicas=3)
    assert high_cpu > low_cpu
    print(f"  CPU low: {low_cpu:.1%}")
    print(f"  CPU high: {high_cpu:.1%}")

    ema = 0.0
    for _ in range(10):
        ema = exponential_moving_average(ema, 100.0, alpha=0.1)
    assert ema > 50.0
    print(f"  EMA: {ema:.1f}ms")

    print("  PASS")



async def test_circuit_breaker():
    print("\n--- Test 2: Circuit Breaker ---")

    cb = CircuitBreaker(
        service_name="payment",
        failure_threshold=0.5,
        window_size=10,
        open_timeout_s=0.2,
    )

    assert cb.state == CircuitState.CLOSED
    assert cb.error_rate == 0.0
    print(f"  Initial: {cb.state.value}")

    allowed = await cb.allow_request()
    assert allowed is True
    print(f"  Closed allows: {allowed}")

    for _ in range(4):
        await cb.record_success()
    for _ in range(6):
        await cb.record_failure()

    assert cb.state == CircuitState.OPEN
    assert cb.error_rate >= 0.5
    print(f"  After failures: {cb.state.value}")

    allowed = await cb.allow_request()
    assert allowed is False
    print(f"  Open allows: {allowed}")

    await asyncio.sleep(0.25)
    allowed = await cb.allow_request()
    assert allowed is True
    assert cb.state == CircuitState.HALF_OPEN
    print(f"  Half-open allows: {allowed}")

    await cb.record_success()
    assert cb.state == CircuitState.CLOSED
    assert cb.error_rate == 0.0
    print(f"  After success: {cb.state.value}")

    for _ in range(10):
        await cb.record_failure()
    assert cb.state == CircuitState.OPEN
    await asyncio.sleep(0.25)

    await cb.allow_request()
    assert cb.state == CircuitState.HALF_OPEN
    await cb.record_failure()
    assert cb.state == CircuitState.OPEN
    print(f"  Failed probe: {cb.state.value}")

    events = []
    cb2 = CircuitBreaker("test", failure_threshold=0.5, window_size=4, open_timeout_s=0.1)
    cb2.on_open(lambda svc, old, new: events.append(f"OPEN:{svc}"))
    cb2.on_close(lambda svc, old, new: events.append(f"CLOSE:{svc}"))

    for _ in range(4):
        await cb2.record_failure()
    assert "OPEN:test" in events

    await asyncio.sleep(0.15)
    await cb2.allow_request()
    await cb2.record_success()
    assert "CLOSE:test" in events
    print(f"  Callbacks: {events}")

    print("  PASS")


# Test 3: Cascade engine

async def test_cascade_engine():
    print("\n--- Test 3: Cascade Engine ---")

    from shared.config import DEPENDENCY_GRAPH

    reverse = build_reverse_graph(DEPENDENCY_GRAPH)

    assert "payment" in reverse.get("inventory", [])
    assert "gateway" in reverse.get("payment", [])
    print(f"  Reverse graph inventory: {reverse.get('inventory')}")
    print(f"  Reverse graph payment: {reverse.get('payment')}")

    upstream = get_upstream_services("inventory", reverse)
    assert "payment" in upstream
    assert "gateway" in upstream
    print(f"  Upstream of inventory: {upstream}")

    failing_state = ServiceState(
        name="inventory",
        error_rate=0.7,
        avg_latency=500.0,
        queue_depth=180,
        active_requests=15,
    )
    impact_direct = compute_cascade_impact(
        failing_service="inventory",
        upstream_service="payment",
        failing_state=failing_state,
        hop_distance=1,
    )
    assert impact_direct["failure_impact"] > 0
    assert impact_direct["latency_impact"] > 0
    print(f"  Impact on payment: +{impact_direct['failure_impact']:.0%} failure, +{impact_direct['latency_impact']:.0f}ms latency")

    impact_indirect = compute_cascade_impact(
        failing_service="inventory",
        upstream_service="gateway",
        failing_state=failing_state,
        hop_distance=2,
    )
    assert impact_indirect["failure_impact"] < impact_direct["failure_impact"]
    print(f"  Impact on gateway: +{impact_indirect['failure_impact']:.0%} failure, +{impact_indirect['latency_impact']:.0f}ms latency")

    healthy_state = ServiceState(
        name="inventory",
        error_rate=0.02,
    )
    impact_healthy = compute_cascade_impact(
        failing_service="inventory",
        upstream_service="payment",
        failing_state=healthy_state,
        hop_distance=1,
    )
    assert impact_healthy["failure_impact"] == 0.0
    print(f"  Healthy cascade: {impact_healthy['failure_impact']:.0%}")

    cascade_eng = CascadeEngine()
    cascades = cascade_eng.evaluate("inventory", failing_state)

    assert len(cascades) > 0
    affected = [c["service"] for c in cascades]
    assert "payment" in affected
    print(f"  Affected services: {affected}")

    assert len(cascade_eng.incidents.active_incidents()) > 0
    incidents = cascade_eng.incidents.active_incidents()
    print(f"  Incidents: {len(incidents)}")

    print("  PASS")


# Test 4: Service runner flow

async def test_service_runner_flow():
    print("\n--- Test 4: End-to-End Service Runner Flow ---")

    from shared.models import Event, EventType
    from shared.event_bus import EventBus
    from shared.state_store import StateStore
    from simulator.service_runner import run_service, retry_storm_detector
    from shared.config import SERVICE_CONFIGS

    local_bus   = EventBus()
    local_store = StateStore()
    cb_reg      = CircuitBreakerRegistry()
    cascade_eng = CascadeEngine()
    event_log   = []

    for name in ["gateway", "auth"]:
        cfg = SERVICE_CONFIGS[name]
        local_bus.register(name, cfg)
        local_store.register(name, replicas=cfg.replicas)
        cb_reg.register(name, failure_threshold=0.6, window_size=10, open_timeout_s=1.0)

    for name in ["gateway", "auth"]:
        cb = cb_reg.get(name)
        cb.on_open(cascade_eng.on_circuit_open)
        cb.on_close(cascade_eng.on_circuit_close)

    gateway_task = asyncio.create_task(
        run_service("gateway", local_bus, local_store, cb_reg, cascade_eng, event_log)
    )
    auth_task = asyncio.create_task(
        run_service("auth", local_bus, local_store, cb_reg, cascade_eng, event_log)
    )

    sent = 0
    for i in range(20):
        event = Event(
            source_service = "user",
            target_service = "gateway",
            event_type     = EventType.REQUEST,
        )
        ok = await local_bus.publish("gateway", event)
        if ok:
            sent += 1

    print(f"  Published {sent}/20 events to gateway")

    await asyncio.sleep(1.5)

    gateway_task.cancel()
    auth_task.cancel()
    try:
        await asyncio.gather(gateway_task, auth_task, return_exceptions=True)
    except Exception:
        pass

    gateway_state = local_store.get_sync("gateway")
    assert gateway_state.total_requests > 0, "Gateway should have processed some requests"
    assert gateway_state.avg_latency > 0, "Gateway avg_latency should be non-zero"
    print(f"  Gateway processed: {gateway_state.total_requests} requests  "
          f"errors={gateway_state.total_errors}  "
          f"avg_latency={gateway_state.avg_latency:.1f}ms")

    assert len(event_log) > 0, "event_log should have entries"
    print(f"  Event log has {len(event_log)} entries. Last 3:")
    for entry in event_log[-3:]:
        print(f"    [{entry['time']}] {entry['message']}")

    print("  PASS")


# Main runner

async def main():
    print("=" * 60)
    print("  Stage 2 — Latency Model, Circuit Breaker, Cascade Engine")
    print("=" * 60)

    await test_latency_model()
    await test_circuit_breaker()
    await test_cascade_engine()
    await test_service_runner_flow()

    print("\n" + "=" * 60)
    print("  All Stage 2 tests passed. Ready for Stage 2 demo.")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
