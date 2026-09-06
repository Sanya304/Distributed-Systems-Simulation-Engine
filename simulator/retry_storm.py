
import time
import math
import random
from collections import defaultdict, deque
from dataclasses import dataclass, field

@dataclass
class BackoffConfig:

    base_delay_s:  float = 0.1
    max_delay_s:   float = 30.0
    multiplier:    float = 2.0
    jitter_factor: float = 0.25
    max_retries:   int   = 3


def compute_backoff(retry_count: int, config: BackoffConfig) -> float:

    # Exponential growth: base * multiplier^retry
    raw_delay = config.base_delay_s * (config.multiplier ** retry_count)

    # Cap at max to prevent absurdly long delays
    capped_delay = min(raw_delay, config.max_delay_s)

    # Add jitter: multiply by (1 + random * jitter_factor)
    # This spreads out retry timing to prevent thundering herd
    jitter = random.uniform(0, config.jitter_factor)
    final_delay = capped_delay * (1 + jitter)

    return final_delay



class RetryStormDetector:


    def __init__(
        self,
        storm_threshold_rps: float = 3.0,
        window_s:            float = 10.0,
    ):

        self.threshold = storm_threshold_rps
        self.window_s  = window_s

        # Per-service sliding window of retry timestamps
        # deque stores unix timestamps of each retry event
        self._retry_times:  dict[str, deque] = defaultdict(deque)

        # Per-service total counters (never reset, for amplification calc)
        self._total_retries:   dict[str, int]   = defaultdict(int)
        self._total_requests:  dict[str, int]   = defaultdict(int)

        # Storm start times — when did each service's storm begin?
        self._storm_started:   dict[str, float] = {}

        # Peak retry rate seen during current/most-recent storm
        self._peak_rate:       dict[str, float] = defaultdict(float)

    def record_retry(self, service_name: str) -> None:

        now = time.time()
        self._retry_times[service_name].append(now)
        self._total_retries[service_name] += 1

        # Update peak if currently storming
        rate = self._current_rate(service_name)
        if rate > self._peak_rate[service_name]:
            self._peak_rate[service_name] = rate

        # Track storm start time
        if self.is_storming(service_name):
            if service_name not in self._storm_started:
                self._storm_started[service_name] = now
        else:
            # Storm ended — reset start time
            self._storm_started.pop(service_name, None)

    def record_request(self, service_name: str) -> None:

        self._total_requests[service_name] += 1

    def is_storming(self, service_name: str) -> bool:

        return self._current_rate(service_name) >= self.threshold

    def retry_rate(self, service_name: str) -> float:

        return round(self._current_rate(service_name), 2)

    def amplification_factor(self, service_name: str) -> float:

        requests = self._total_requests.get(service_name, 0)
        retries  = self._total_retries.get(service_name, 0)
        if requests == 0:
            return 1.0
        return round(1.0 + (retries / requests), 2)

    def storm_duration_s(self, service_name: str) -> float:

        start = self._storm_started.get(service_name)
        if start is None:
            return 0.0
        return round(time.time() - start, 1)

    def peak_rate(self, service_name: str) -> float:

        return round(self._peak_rate.get(service_name, 0.0), 2)

    def storming_services(self) -> list[dict]:

        result = []
        for name in list(self._retry_times.keys()):
            rate = self._current_rate(name)
            if rate >= self.threshold:
                result.append({
                    "service":       name,
                    "retry_rate":    round(rate, 2),
                    "amplification": self.amplification_factor(name),
                    "duration_s":    self.storm_duration_s(name),
                    "peak_rate":     self.peak_rate(name),
                })
        return sorted(result, key=lambda x: x["retry_rate"], reverse=True)

    def all_stats(self) -> dict:

        stats = {}
        for name in set(list(self._retry_times.keys()) + list(self._total_retries.keys())):
            stats[name] = {
                "retry_rate":    self.retry_rate(name),
                "amplification": self.amplification_factor(name),
                "is_storming":   self.is_storming(name),
                "duration_s":    self.storm_duration_s(name),
                "total_retries": self._total_retries.get(name, 0),
                "peak_rate":     self.peak_rate(name),
            }
        return stats

    def _current_rate(self, service_name: str) -> float:

        if service_name not in self._retry_times:
            return 0.0

        times  = self._retry_times[service_name]
        cutoff = time.time() - self.window_s

        # Prune expired entries from the left (oldest)
        while times and times[0] < cutoff:
            times.popleft()

        if not times:
            return 0.0

        return len(times) / self.window_s



storm_detector = RetryStormDetector(storm_threshold_rps=3.0, window_s=10.0)
