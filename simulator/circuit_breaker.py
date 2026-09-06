"""Circuit Breaker pattern — stops sending requests to failing services."""

import asyncio
import time
from collections import deque
from shared.models import CircuitState


class CircuitBreaker:

    def __init__(
        self,
        service_name:      str,
        failure_threshold: float = 0.5,
        window_size:       int   = 20,
        open_timeout_s:    float = 10.0,
        half_open_max:     int   = 1,
    ):
        self.service_name      = service_name
        self.failure_threshold = failure_threshold
        self.window_size       = window_size
        self.open_timeout_s    = open_timeout_s
        self.half_open_max     = half_open_max

        self._state = CircuitState.CLOSED
        self._window: deque[bool] = deque(maxlen=window_size)
        self._opened_at: float | None = None
        self._half_open_probes: int = 0
        self._on_open:      list[callable] = []
        self._on_close:     list[callable] = []
        self._on_half_open: list[callable] = []
        self._lock = asyncio.Lock()


    @property
    def state(self) -> CircuitState:
        return self._state

    @property
    def error_rate(self) -> float:
        if not self._window:
            return 0.0
        failures = sum(1 for outcome in self._window if not outcome)
        return failures / len(self._window)

    @property
    def window_count(self) -> int:
        return len(self._window)

    async def allow_request(self) -> bool:
        async with self._lock:
            if self._state == CircuitState.CLOSED:
                return True
            elif self._state == CircuitState.OPEN:
                if self._opened_at and (time.time() - self._opened_at) >= self.open_timeout_s:
                    await self._transition_to(CircuitState.HALF_OPEN)
                    self._half_open_probes = 0
                    return True
                return False
            elif self._state == CircuitState.HALF_OPEN:
                if self._half_open_probes < self.half_open_max:
                    self._half_open_probes += 1
                    return True
                return False

        return False

    async def record_success(self) -> None:
        async with self._lock:
            self._window.append(True)
            if self._state == CircuitState.HALF_OPEN:
                await self._transition_to(CircuitState.CLOSED)

    async def record_failure(self) -> None:
        async with self._lock:
            self._window.append(False)
            if self._state == CircuitState.HALF_OPEN:
                await self._transition_to(CircuitState.OPEN)
            elif self._state == CircuitState.CLOSED:
                if len(self._window) >= self.window_size // 2:
                    current_error_rate = self.error_rate
                    if current_error_rate >= self.failure_threshold:
                        await self._transition_to(CircuitState.OPEN)

    def on_open(self, callback: callable) -> None:
        self._on_open.append(callback)

    def on_close(self, callback: callable) -> None:
        self._on_close.append(callback)

    def on_half_open(self, callback: callable) -> None:
        self._on_half_open.append(callback)

    def stats(self) -> dict:
        return {
            "service":          self.service_name,
            "state":            self._state.value,
            "error_rate":       round(self.error_rate, 3),
            "window_count":     self.window_count,
            "window_size":      self.window_size,
            "failure_threshold":self.failure_threshold,
            "opened_at":        self._opened_at,
        }


    async def _transition_to(self, new_state: CircuitState) -> None:
        old_state  = self._state
        self._state = new_state

        if new_state == CircuitState.OPEN:
            self._opened_at = time.time()
            for cb in self._on_open:
                cb(self.service_name, old_state, new_state)

        elif new_state == CircuitState.CLOSED:
            self._opened_at = None
            self._window.clear()
            for cb in self._on_close:
                cb(self.service_name, old_state, new_state)

        elif new_state == CircuitState.HALF_OPEN:
            for cb in self._on_half_open:
                cb(self.service_name, old_state, new_state)



class CircuitBreakerRegistry:

    def __init__(self):
        self._breakers: dict[str, CircuitBreaker] = {}

    def register(
        self,
        service_name:      str,
        failure_threshold: float = 0.5,
        window_size:       int   = 20,
        open_timeout_s:    float = 10.0,
    ) -> CircuitBreaker:
        cb = CircuitBreaker(
            service_name      = service_name,
            failure_threshold = failure_threshold,
            window_size       = window_size,
            open_timeout_s    = open_timeout_s,
        )
        self._breakers[service_name] = cb
        return cb

    def get(self, service_name: str) -> CircuitBreaker | None:
        return self._breakers.get(service_name)

    def all_states(self) -> dict[str, str]:
        return {
            name: cb.state.value
            for name, cb in self._breakers.items()
        }

    def all_stats(self) -> dict[str, dict]:
        return {
            name: cb.stats()
            for name, cb in self._breakers.items()
        }


registry = CircuitBreakerRegistry()
