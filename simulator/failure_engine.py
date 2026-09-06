"""Cascading failure propagation engine."""

import time
from collections import defaultdict, deque
from shared.models import ServiceConfig, ServiceState
from shared.config import DEPENDENCY_GRAPH, SERVICE_CONFIGS



def build_reverse_graph(dep_graph: dict[str, list[str]]) -> dict[str, list[str]]:
    reverse: dict[str, list[str]] = defaultdict(list)
    for service, dependencies in dep_graph.items():
        for dep in dependencies:
            reverse[dep].append(service)
    return dict(reverse)


def get_upstream_services(failed_service: str, reverse_graph: dict[str, list[str]]) -> list[str]:
    visited = set()
    queue   = deque([failed_service])
    result  = []

    while queue:
        current = queue.popleft()
        for upstream in reverse_graph.get(current, []):
            if upstream not in visited:
                visited.add(upstream)
                result.append(upstream)
                queue.append(upstream)

    return result


def get_downstream_services(service: str, dep_graph: dict[str, list[str]]) -> list[str]:
    visited = set()
    queue   = deque([service])
    result  = []

    while queue:
        current = queue.popleft()
        for downstream in dep_graph.get(current, []):
            if downstream not in visited:
                visited.add(downstream)
                result.append(downstream)
                queue.append(downstream)

    return result



CASCADE_FAILURE_FACTOR = 0.4
CASCADE_LATENCY_FACTOR = 0.5
CASCADE_TRIGGER_THRESHOLD = 0.15


def compute_cascade_impact(
    failing_service:  str,
    upstream_service: str,
    failing_state:    ServiceState,
    hop_distance:     int,
) -> dict:
    if failing_state.error_rate < CASCADE_TRIGGER_THRESHOLD:
        return {"failure_impact": 0.0, "latency_impact": 0.0, "severity": "none"}

    attenuation = 1.0 / (2 ** (hop_distance - 1))

    failure_impact = (
        failing_state.error_rate
        * CASCADE_FAILURE_FACTOR
        * attenuation
    )

    config = SERVICE_CONFIGS.get(failing_service)
    base_latency = config.base_latency_ms if config else 50.0
    latency_impact = (
        failing_state.avg_latency
        * CASCADE_LATENCY_FACTOR
        * attenuation
        * (1 + failing_state.queue_depth / 100)  # extra penalty for queued-up service
    )

    if failure_impact > 0.3:
        severity = "high"
    elif failure_impact > 0.15:
        severity = "medium"
    else:
        severity = "low"

    return {
        "failure_impact":  round(failure_impact, 3),
        "latency_impact":  round(latency_impact, 1),
        "severity":        severity,
    }



class IncidentTracker:

    def __init__(self):
        self._active: list[dict] = []
        self._resolved: list[dict] = []

    def open_incident(
        self,
        service:       str,
        incident_type: str,
        description:   str,
        severity:      str = "medium",
    ) -> str:
        for existing in self._active:
            if existing["service"] == service and existing["type"] == incident_type:
                return existing["id"]

        incident_id = f"{service}_{incident_type}_{int(time.time())}"
        incident = {
            "id":          incident_id,
            "service":     service,
            "type":        incident_type,
            "description": description,
            "severity":    severity,
            "started_at":  time.time(),
            "resolved_at": None,
        }
        self._active.append(incident)
        return incident_id

    def resolve_incident(self, service: str, incident_type: str) -> None:
        still_active = []
        for incident in self._active:
            if incident["service"] == service and incident["type"] == incident_type:
                incident["resolved_at"] = time.time()
                self._resolved.append(incident)
            else:
                still_active.append(incident)
        self._active = still_active

    def resolve_all(self, service: str) -> None:
        still_active = []
        for incident in self._active:
            if incident["service"] == service:
                incident["resolved_at"] = time.time()
                self._resolved.append(incident)
            else:
                still_active.append(incident)
        self._active = still_active

    def active_incidents(self) -> list[dict]:
        return list(self._active)

    def recent_resolved(self, limit: int = 10) -> list[dict]:
        return sorted(self._resolved, key=lambda i: i["resolved_at"] or 0, reverse=True)[:limit]



class CascadeEngine:

    def __init__(self):
        self._reverse_graph = build_reverse_graph(DEPENDENCY_GRAPH)
        self.incidents = IncidentTracker()

    def evaluate(
        self,
        service_name:  str,
        current_state: ServiceState,
    ) -> list[dict]:
        cascades = []

        if current_state.error_rate < CASCADE_TRIGGER_THRESHOLD:
            self.incidents.resolve_incident(service_name, "cascade")
            return cascades

        visited_distances: dict[str, int] = {}
        queue = deque([(service_name, 0)])

        while queue:
            current, distance = queue.popleft()
            for upstream in self._reverse_graph.get(current, []):
                if upstream not in visited_distances:
                    hop = distance + 1
                    visited_distances[upstream] = hop
                    queue.append((upstream, hop))

        for upstream_service, hop_distance in visited_distances.items():
            impact = compute_cascade_impact(
                failing_service  = service_name,
                upstream_service = upstream_service,
                failing_state    = current_state,
                hop_distance     = hop_distance,
            )

            if impact["failure_impact"] > 0:
                impact["service"]      = upstream_service
                impact["hop_distance"] = hop_distance
                cascades.append(impact)

                if impact["severity"] in ("medium", "high"):
                    self.incidents.open_incident(
                        service       = upstream_service,
                        incident_type = "cascade",
                        description   = (
                            f"{upstream_service} degraded because "
                            f"{service_name} is failing "
                            f"(error_rate={current_state.error_rate:.0%})"
                        ),
                        severity      = impact["severity"],
                    )

        return cascades

    def on_circuit_open(self, service_name: str, old_state, new_state) -> None:
        self.incidents.open_incident(
            service       = service_name,
            incident_type = "circuit_open",
            description   = f"{service_name} circuit breaker OPENED — service failing",
            severity      = "high",
        )

    def on_circuit_close(self, service_name: str, old_state, new_state) -> None:
        self.incidents.resolve_incident(service_name, "circuit_open")
        self.incidents.resolve_incident(service_name, "cascade")


engine = CascadeEngine()
