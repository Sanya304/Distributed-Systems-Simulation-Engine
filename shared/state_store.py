"""In-memory shared state store — holds live runtime metrics for every service."""

from __future__ import annotations
import asyncio
from shared.models import ServiceState, CircuitState


class StateStore:

    def __init__(self):
        self._states: dict[str, ServiceState] = {}
        self._lock = asyncio.Lock()

    def register(self, name: str, replicas: int = 1) -> None:
        if name not in self._states:
            self._states[name] = ServiceState(name=name, replicas=replicas)

    async def get(self, name: str) -> ServiceState:
        async with self._lock:
            if name not in self._states:
                raise KeyError(f"Service '{name}' not registered in state store")
            s = self._states[name]
            return ServiceState(**s.__dict__)

    async def update(self, name: str, **kwargs) -> None:
        async with self._lock:
            state = self._states.get(name)
            if state is None:
                raise KeyError(f"Service '{name}' not registered in state store")
            for key, value in kwargs.items():
                if hasattr(state, key):
                    setattr(state, key, value)

    async def increment(self, name: str, field: str, amount: int = 1) -> int:
        async with self._lock:
            state = self._states[name]
            current = getattr(state, field)
            new_value = current + amount
            setattr(state, field, new_value)
            return new_value

    async def snapshot(self) -> dict[str, ServiceState]:
        async with self._lock:
            return {
                name: ServiceState(**s.__dict__)
                for name, s in self._states.items()
            }

    async def all_circuit_states(self) -> dict[str, str]:
        async with self._lock:
            return {
                name: state.circuit_state.value
                for name, state in self._states.items()
            }

    def get_sync(self, name: str) -> ServiceState | None:
        return self._states.get(name)

    def all_names(self) -> list[str]:
        return list(self._states.keys())


store = StateStore()
