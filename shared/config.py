"""Static configuration for all simulated services."""

from shared.models import ServiceConfig



SERVICE_CONFIGS: dict[str, ServiceConfig] = {

    "gateway": ServiceConfig(
        name="gateway",
        base_latency_ms=10,
        failure_probability=0.01,
        max_queue_size=500,
        timeout_ms=3000,
        dependencies=[],
        replicas=2,
        cpu_threshold=0.75,
    ),

    "auth": ServiceConfig(
        name="auth",
        base_latency_ms=30,
        failure_probability=0.02,
        max_queue_size=300,
        timeout_ms=2000,
        dependencies=["gateway"],
        replicas=2,
        cpu_threshold=0.80,
    ),

    "payment": ServiceConfig(
        name="payment",
        base_latency_ms=80,
        failure_probability=0.05,
        max_queue_size=200,
        timeout_ms=5000,
        dependencies=["auth"],
        replicas=3,
        cpu_threshold=0.70,
    ),

    "inventory": ServiceConfig(
        name="inventory",
        base_latency_ms=50,
        failure_probability=0.03,
        max_queue_size=400,
        timeout_ms=3000,
        dependencies=["payment"],
        replicas=2,
        cpu_threshold=0.80,
    ),

    "notification": ServiceConfig(
        name="notification",
        base_latency_ms=20,
        failure_probability=0.04,
        max_queue_size=1000,
        timeout_ms=2000,
        dependencies=["payment"],
        replicas=1,
        cpu_threshold=0.90,
    ),
}



DEPENDENCY_GRAPH: dict[str, list[str]] = {
    "gateway":      ["auth", "payment"],
    "auth":         [],
    "payment":      ["inventory", "notification"],
    "inventory":    [],
    "notification": [],
}



SERVICE_ORDER = ["gateway", "auth", "payment", "inventory", "notification"]
