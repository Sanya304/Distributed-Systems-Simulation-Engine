

import random
import math
from shared.models import ServiceConfig, ServiceState


def compute_latency(config: ServiceConfig, state: ServiceState) -> float:


    base = config.base_latency_ms

    queue_ratio   = state.queue_depth / max(config.max_queue_size, 1)
    queue_penalty = queue_ratio * base * 2.0


    cpu_penalty = 0.0
    if state.cpu_usage > config.cpu_threshold:
        # How far above threshold are we? (0.0 to 1.0)
        overflow = (state.cpu_usage - config.cpu_threshold) / max(1.0 - config.cpu_threshold, 0.01)
        # Squared so the effect accelerates as CPU approaches 100%
        cpu_penalty = (overflow ** 2) * base * 5.0



    jitter = random.gauss(0, base * 0.1)

    total = base + queue_penalty + cpu_penalty + jitter


    return max(1.0, total)


def compute_failure_probability(config: ServiceConfig, state: ServiceState) -> float:

    base_prob = config.failure_probability


    queue_ratio    = state.queue_depth / max(config.max_queue_size, 1)
    queue_pressure = queue_ratio * base_prob * 2.0

    cpu_pressure = 0.0
    if state.cpu_usage > config.cpu_threshold:
        overflow     = state.cpu_usage - config.cpu_threshold
        cpu_pressure = overflow * base_prob * 3.0

    effective = base_prob + queue_pressure + cpu_pressure


    return min(effective, 0.95)


def compute_cpu_usage(state: ServiceState, replicas: int, max_rps_per_replica: int = 10) -> float:

    capacity = max(replicas * max_rps_per_replica, 1)


    raw_cpu = state.active_requests / capacity

    noise = random.gauss(0, 0.02)

    return max(0.0, min(1.0, raw_cpu + noise))


def exponential_moving_average(current: float, new_sample: float, alpha: float = 0.1) -> float:

    return alpha * new_sample + (1 - alpha) * current
