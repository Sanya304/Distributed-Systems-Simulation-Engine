"""Token bucket rate limiter — in-memory, no Redis needed."""

import time
import asyncio
from collections import defaultdict


class TokenBucket:

    def __init__(self, max_tokens: float, refill_rate: float):
        self.max_tokens   = max_tokens
        self.refill_rate  = refill_rate
        self.tokens       = float(max_tokens)
        self.last_refill  = time.monotonic()

    def consume(self) -> bool:
        self._refill()

        if self.tokens >= 1.0:
            self.tokens -= 1.0
            return True
        return False

    def _refill(self) -> None:
        now     = time.monotonic()
        elapsed = now - self.last_refill
        tokens_to_add    = elapsed * self.refill_rate
        self.tokens      = min(self.max_tokens, self.tokens + tokens_to_add)
        self.last_refill = now

    @property
    def available(self) -> float:
        self._refill()
        return self.tokens


class RateLimiter:

    def __init__(
        self,
        requests_per_second: float = 10.0,
        burst:               float = 20.0,
        cleanup_interval_s:  float = 60.0,
    ):
        self.rps              = requests_per_second
        self.burst            = burst
        self.cleanup_interval = cleanup_interval_s

        self._buckets:       dict[str, TokenBucket] = {}
        self._last_seen:     dict[str, float]        = {}
        self._last_cleanup:  float                   = time.monotonic()

        self.total_allowed   = 0
        self.total_rejected  = 0

    def is_allowed(self, client_ip: str) -> bool:
        self._maybe_cleanup()

        if client_ip not in self._buckets:
            self._buckets[client_ip] = TokenBucket(
                max_tokens   = self.burst,
                refill_rate  = self.rps,
            )

        self._last_seen[client_ip] = time.monotonic()

        allowed = self._buckets[client_ip].consume()
        if allowed:
            self.total_allowed += 1
        else:
            self.total_rejected += 1
        return allowed

    def client_stats(self, client_ip: str) -> dict:
        bucket = self._buckets.get(client_ip)
        if bucket is None:
            return {"tokens": self.burst, "limit": self.rps}
        return {
            "tokens":    round(bucket.available, 2),
            "limit":     self.rps,
            "burst":     self.burst,
        }

    def stats(self) -> dict:
        return {
            "active_clients":  len(self._buckets),
            "total_allowed":   self.total_allowed,
            "total_rejected":  self.total_rejected,
            "rejection_rate":  round(
                self.total_rejected / max(self.total_allowed + self.total_rejected, 1), 3
            ),
        }

    def _maybe_cleanup(self) -> None:
        now = time.monotonic()
        if now - self._last_cleanup < self.cleanup_interval:
            return

        cutoff  = now - self.cleanup_interval
        stale   = [ip for ip, t in self._last_seen.items() if t < cutoff]
        for ip in stale:
            del self._buckets[ip]
            del self._last_seen[ip]

        self._last_cleanup = now


limiter = RateLimiter(requests_per_second=50.0, burst=100.0)
