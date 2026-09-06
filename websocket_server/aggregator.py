"""Metrics aggregator for simulation snapshots."""

import time
import json
from collections import deque

from shared.state_store import StateStore
from shared.event_bus import EventBus
from shared.config import SERVICE_CONFIGS, SERVICE_ORDER
from simulator.circuit_breaker import CircuitBreakerRegistry
from simulator.failure_engine import CascadeEngine
from simulator.retry_storm import RetryStormDetector
from simulator.autoscaler import Autoscaler


class MetricsAggregator:

    def __init__(
        self,
        store:        StateStore,
        bus:          EventBus,
        cb_registry:  CircuitBreakerRegistry,
        cascade:      CascadeEngine,
        storm:        RetryStormDetector,
        scaler:       Autoscaler,
        event_log:    list,
    ):
        self.store       = store
        self.bus         = bus
        self.cb_registry = cb_registry
        self.cascade     = cascade
        self.storm       = storm
        self.scaler      = scaler
        self.event_log   = event_log

        self._history: deque = deque(maxlen=60)
        self._prev_total_requests: dict[str, int] = {}
        self._prev_sample_time:    float          = time.time()
        self._trace_history: deque = deque(maxlen=50)
        self._seq: int = 0

    def build_snapshot(self) -> dict:

        now = time.time()
        self._seq += 1

        services    = {}
        circuit_map = {}
        total_req   = 0
        total_err   = 0

        for name in SERVICE_ORDER:
            state = self.store.get_sync(name)
            cb    = self.cb_registry.get(name)
            cfg   = SERVICE_CONFIGS[name]

            if state is None:
                continue

            cb_state    = cb.state.value  if cb else "CLOSED"
            cb_err_rate = cb.error_rate   if cb else 0.0
            cb_window   = cb.window_count if cb else 0

            queue_depth = self.bus.queue_depth(name)
            queue_sat   = round(queue_depth / max(cfg.max_queue_size, 1), 3)

            # Throughput: requests since last sample / elapsed seconds
            elapsed = max(now - self._prev_sample_time, 0.001)
            prev_req = self._prev_total_requests.get(name, state.total_requests)
            rps      = round((state.total_requests - prev_req) / elapsed, 2)

            services[name] = {
                # Identity
                "name":            name,
                "replicas":        state.replicas,
                "default_replicas":cfg.replicas,
                "is_alive":        state.is_alive,

                # Latency
                "avg_latency":     round(state.avg_latency, 2),
                "base_latency":    cfg.base_latency_ms,

                # Error metrics
                "error_rate":      round(state.error_rate, 4),
                "error_pct":       round(state.error_rate * 100, 2),
                "total_errors":    state.total_errors,

                # Load metrics
                "total_requests":  state.total_requests,
                "active_requests": state.active_requests,
                "cpu_usage":       round(state.cpu_usage, 4),
                "cpu_pct":         round(state.cpu_usage * 100, 2),
                "queue_depth":     queue_depth,
                "queue_saturation":queue_sat,
                "max_queue_size":  cfg.max_queue_size,
                "throughput_rps":  rps,

                # Circuit breaker
                "circuit_state":   cb_state,
                "cb_error_rate":   round(cb_err_rate, 4),
                "cb_window_count": cb_window,

                # Chaos
                "extra_latency_ms":state.extra_latency_ms,

                # Retry storm
                "retry_rate":      self.storm.retry_rate(name),
                "is_storming":     self.storm.is_storming(name),
                "amplification":   self.storm.amplification_factor(name),
            }
            circuit_map[name] = cb_state
            total_req += state.total_requests
            total_err += state.total_errors

        # Update prev state for next throughput calculation
        self._prev_total_requests = {
            name: self.store.get_sync(name).total_requests
            for name in SERVICE_ORDER
            if self.store.get_sync(name) is not None
        }
        self._prev_sample_time = now

        system_error_rate = round(total_err / max(total_req, 1), 4)

        incidents = [
            {
                "id":          inc["id"],
                "service":     inc["service"],
                "type":        inc["type"],
                "description": inc["description"],
                "severity":    inc["severity"],
                "started_at":  inc["started_at"],
                "age_s":       round(now - inc["started_at"], 1),
            }
            for inc in self.cascade.incidents.active_incidents()
        ]

        storms = self.storm.storming_services()

        scale_stats = self.scaler.stats()

        recent_events = list(self.event_log[-20:])

        from shared.config import DEPENDENCY_GRAPH
        edges = [
            {"source": src, "target": tgt}
            for src, targets in DEPENDENCY_GRAPH.items()
            for tgt in targets
        ]

        snapshot = {
            "seq":               self._seq,
            "timestamp":         now,
            "services":          services,
            "circuit_states":    circuit_map,
            "system": {
                "total_requests":    total_req,
                "total_errors":      total_err,
                "error_rate":        system_error_rate,
                "error_pct":         round(system_error_rate * 100, 2),
            },
            "incidents":         incidents,
            "retry_storms":      storms,
            "autoscaler":        scale_stats,
            "recent_events":     recent_events,
            "dependency_graph":  edges,
        }

        self._history.append({
            "timestamp": now,
            "services": {
                name: {
                    "avg_latency": svc["avg_latency"],
                    "error_rate":  svc["error_rate"],
                    "cpu_pct":     svc["cpu_pct"],
                    "queue_depth": svc["queue_depth"],
                    "throughput_rps": svc["throughput_rps"],
                }
                for name, svc in services.items()
            },
            "system_error_rate": system_error_rate,
        })

        return snapshot

    def get_history(self) -> list:

        return list(self._history)

    def to_json(self, snapshot: dict) -> str:
        return json.dumps(snapshot, default=str)
