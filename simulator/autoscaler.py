

import asyncio
import time
import logging
from dataclasses import dataclass, field

from shared.state_store import StateStore
from shared.event_bus import EventBus
from shared.config import SERVICE_CONFIGS, SERVICE_ORDER


logger = logging.getLogger("simulator.autoscaler")



MIN_REPLICAS      = 1
MAX_REPLICAS      = 10


POLL_INTERVAL_S   = 3.0

SCALE_UP_COOLDOWN_S   = 10.0


SCALE_DOWN_COOLDOWN_S = 30.0


SCALE_DOWN_STABILITY_CHECKS = 3


QUEUE_SATURATION_THRESHOLD = 0.7


SCALE_DOWN_CPU_RATIO = 0.3



@dataclass
class ScalingEvent:

    service:         str
    direction:       str       # "UP" or "DOWN"
    old_replicas:    int
    new_replicas:    int
    reason:          str       # human-readable explanation
    timestamp:       float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        return {
            "service":      self.service,
            "direction":    self.direction,
            "old_replicas": self.old_replicas,
            "new_replicas": self.new_replicas,
            "reason":       self.reason,
            "timestamp":    self.timestamp,
        }


@dataclass
class ServiceScalerState:

    last_scale_up_time:    float = 0.0    # unix timestamp of last scale-up
    last_scale_down_time:  float = 0.0    # unix timestamp of last scale-down

    stable_below_checks:   int   = 0



class Autoscaler:


    def __init__(
        self,
        store:     StateStore,
        bus:       EventBus,
        event_log: list,
    ):

        self.store     = store
        self.bus       = bus
        self.event_log = event_log


        self._scaler_states: dict[str, ServiceScalerState] = {
            name: ServiceScalerState()
            for name in SERVICE_CONFIGS
        }


        self.scaling_history: list[ScalingEvent] = []


        self.total_scale_ups   = 0
        self.total_scale_downs = 0

    async def run(self) -> None:

        logger.info(f"Autoscaler started (poll={POLL_INTERVAL_S}s)")

        while True:
            await asyncio.sleep(POLL_INTERVAL_S)

            for service_name in SERVICE_ORDER:
                await self._evaluate_service(service_name)

    async def _evaluate_service(self, service_name: str) -> None:

        config      = SERVICE_CONFIGS[service_name]
        state       = self.store.get_sync(service_name)
        scaler_st   = self._scaler_states[service_name]

        if state is None:
            return

        now           = time.time()
        current_reps  = state.replicas
        queue_depth   = self.bus.queue_depth(service_name)
        queue_sat     = queue_depth / max(config.max_queue_size, 1)


        should_scale_up = (
            state.cpu_usage > config.cpu_threshold
            or queue_sat > QUEUE_SATURATION_THRESHOLD
        )

        if (
            should_scale_up
            and current_reps < MAX_REPLICAS
            and (now - scaler_st.last_scale_up_time) > SCALE_UP_COOLDOWN_S
        ):
            new_reps = min(current_reps + 1, MAX_REPLICAS)
            reason = (
                f"cpu={state.cpu_usage:.0%} > threshold={config.cpu_threshold:.0%}"
                if state.cpu_usage > config.cpu_threshold
                else f"queue={queue_sat:.0%} > 70%"
            )
            await self._scale(service_name, current_reps, new_reps, "UP", reason)
            scaler_st.last_scale_up_time   = now
            scaler_st.stable_below_checks  = 0
            return


        below_threshold = (
            state.cpu_usage < config.cpu_threshold * SCALE_DOWN_CPU_RATIO
            and queue_sat < 0.2
        )

        if below_threshold:
            # Increment stability counter — must stay below for N polls
            scaler_st.stable_below_checks += 1
        else:
            # Metrics went back up — reset the stability counter
            scaler_st.stable_below_checks = 0

        if (
            scaler_st.stable_below_checks >= SCALE_DOWN_STABILITY_CHECKS
            and current_reps > MIN_REPLICAS
            and (now - scaler_st.last_scale_down_time) > SCALE_DOWN_COOLDOWN_S
        ):
            new_reps = max(current_reps - 1, MIN_REPLICAS)
            reason = (
                f"cpu={state.cpu_usage:.0%} < {config.cpu_threshold * SCALE_DOWN_CPU_RATIO:.0%} "
                f"for {scaler_st.stable_below_checks} polls"
            )
            await self._scale(service_name, current_reps, new_reps, "DOWN", reason)
            scaler_st.last_scale_down_time = now
            scaler_st.stable_below_checks  = 0

    async def _scale(
        self,
        service_name:  str,
        old_replicas:  int,
        new_replicas:  int,
        direction:     str,
        reason:        str,
    ) -> None:


        await self.store.update(service_name, replicas=new_replicas)

        event = ScalingEvent(
            service      = service_name,
            direction    = direction,
            old_replicas = old_replicas,
            new_replicas = new_replicas,
            reason       = reason,
        )

        # Keep only last 50 events
        self.scaling_history.append(event)
        if len(self.scaling_history) > 50:
            self.scaling_history.pop(0)

        if direction == "UP":
            self.total_scale_ups += 1
            color = "cyan"
            arrow = "↑"
        else:
            self.total_scale_downs += 1
            color = "grey"
            arrow = "↓"

        msg = (
            f"SCALE {arrow} {service_name:<14} "
            f"{old_replicas}→{new_replicas} replicas  ({reason})"
        )
        self._log(msg, color)
        logger.info(msg)

    def recent_events(self, limit: int = 10) -> list[dict]:

        return [e.to_dict() for e in reversed(self.scaling_history[-limit:])]

    def stats(self) -> dict:

        current_replicas = {}
        for name in SERVICE_CONFIGS:
            state = self.store.get_sync(name)
            current_replicas[name] = state.replicas if state else 1

        return {
            "total_scale_ups":   self.total_scale_ups,
            "total_scale_downs": self.total_scale_downs,
            "current_replicas":  current_replicas,
            "recent_events":     self.recent_events(5),
        }

    def _log(self, message: str, color: str = "white") -> None:

        self.event_log.append({
            "time":    time.strftime("%H:%M:%S"),
            "message": message,
            "color":   color,
        })
        if len(self.event_log) > 200:
            self.event_log.pop(0)
