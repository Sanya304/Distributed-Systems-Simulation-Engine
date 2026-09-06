
import asyncio
import sys
import random
import time

sys.path.insert(0, "/Users/skumari26/project")

from shared.models import Event, EventType, CircuitState
from shared.event_bus import EventBus
from shared.state_store import StateStore
from shared.config import SERVICE_CONFIGS, DEPENDENCY_GRAPH, SERVICE_ORDER

from simulator.circuit_breaker import CircuitBreakerRegistry
from simulator.failure_engine import CascadeEngine
from simulator.service_runner import run_service, retry_storm_detector
from simulator.autoscaler import Autoscaler
from traffic_generator.generator import TrafficGenerator
from traffic_generator.patterns import demo_pattern



class C:
    RESET   = "\033[0m"
    BOLD    = "\033[1m"
    RED     = "\033[91m"
    GREEN   = "\033[92m"
    YELLOW  = "\033[93m"
    BLUE    = "\033[94m"
    MAGENTA = "\033[95m"
    CYAN    = "\033[96m"
    WHITE   = "\033[97m"
    GREY    = "\033[90m"
    BG_RED  = "\033[41m"
    BG_GRN  = "\033[42m"
    BG_YEL  = "\033[43m"



def render(local_store: StateStore, cb_reg: CircuitBreakerRegistry,
           cascade_eng: CascadeEngine, scaler: Autoscaler, stats: dict, event_log: list):
    print("\033[H\033[J", end="")

    elapsed     = int(time.time() - stats["start_time"])
    total_req   = stats["total_requests"]
    total_err   = stats["total_errors"]
    sys_err_pct = (total_err / max(total_req, 1)) * 100

    print(f"{C.BOLD}{C.CYAN}╔══════════════════════════════════════════════════════════════╗{C.RESET}")
    print(f"{C.BOLD}{C.CYAN}║    Distributed Simulation Engine  —  Stage 2 Demo           ║{C.RESET}")
    print(f"{C.BOLD}{C.CYAN}║    Dynamic Latency · Circuit Breaker · Cascade Failures      ║{C.RESET}")
    print(f"{C.BOLD}{C.CYAN}╚══════════════════════════════════════════════════════════════╝{C.RESET}")
    err_color    = C.GREEN if sys_err_pct < 5 else (C.YELLOW if sys_err_pct < 15 else C.RED)
    current_mode = stats.get("current_mode", "")
    print(f"  {C.GREY}Elapsed: {elapsed:>4}s   "
          f"Requests: {C.WHITE}{total_req:>6}{C.GREY}   "
          f"Errors: {C.RED}{total_err:>5}{C.GREY}   "
          f"Sys Error Rate: {err_color}{sys_err_pct:>5.1f}%{C.RESET}")
    print(f"  {C.BOLD}Traffic: {C.CYAN}{current_mode}{C.RESET}")
    print()

    print(f"{C.BOLD}  {'Service':<14} {'Circuit':<11} {'Queue':<14} {'Latency':>9} {'Error%':>7} {'CPU':>6} {'Rep':>4}{C.RESET}")
    print(f"  {'─'*14} {'─'*11} {'─'*14} {'─'*9} {'─'*7} {'─'*6} {'─'*4}")

    for name in SERVICE_ORDER:
        state = local_store.get_sync(name)
        if state is None:
            continue
        cfg = SERVICE_CONFIGS[name]
        cb  = cb_reg.get(name)

        cb_state = cb.state if cb else CircuitState.CLOSED
        cb_err   = cb.error_rate if cb else 0.0
        cb_color = {
            CircuitState.CLOSED:    C.GREEN,
            CircuitState.OPEN:      C.RED,
            CircuitState.HALF_OPEN: C.YELLOW,
        }[cb_state]
        cb_str = f"{cb_color}{cb_state.value:<8}{C.RESET} {C.GREY}{cb_err:>4.0%}{C.RESET}"

        qbar = _qbar(state.queue_depth, cfg.max_queue_size)

        lc = C.GREEN if state.avg_latency < 100 else (C.YELLOW if state.avg_latency < 400 else C.RED)
        lat_str = f"{lc}{state.avg_latency:>7.1f}ms{C.RESET}"

        err_str = f"{_ecol(state.error_rate)}{state.error_rate*100:>6.1f}%{C.RESET}"

        cc = C.GREEN if state.cpu_usage < 0.7 else (C.YELLOW if state.cpu_usage < 0.9 else C.RED)
        cpu_str = f"{cc}{state.cpu_usage*100:>5.1f}%{C.RESET}"

        default_reps = cfg.replicas
        rep_col = C.CYAN if state.replicas > default_reps else (C.GREY if state.replicas < default_reps else C.WHITE)
        rep_str = f"{rep_col}{state.replicas:>3}x{C.RESET}"

        storm_tag = f" {C.MAGENTA}⚡{C.RESET}" if retry_storm_detector.is_storming(name) else "  "

        print(f"  {C.BOLD}{name:<14}{C.RESET} {cb_str} {qbar} {lat_str} {err_str} {cpu_str} {rep_str}{storm_tag}")

    print()
    print(f"{C.BOLD}  Dependency Graph:{C.RESET}  "
          f"{C.GREEN}■ CLOSED{C.RESET}  {C.YELLOW}■ HALF_OPEN{C.RESET}  {C.RED}■ OPEN{C.RESET}")

    def svc_label(n):
        s  = local_store.get_sync(n)
        cb = cb_reg.get(n)
        if s is None:
            return n
        col = {
            CircuitState.CLOSED:    C.GREEN,
            CircuitState.OPEN:      C.RED,
            CircuitState.HALF_OPEN: C.YELLOW,
        }[cb.state if cb else CircuitState.CLOSED]
        storm_tag = f" {C.MAGENTA}⚡STORM{C.RESET}" if retry_storm_detector.is_storming(n) else ""
        return f"{col}{C.BOLD}{n}{C.RESET}{storm_tag}"

    print(f"  {svc_label('gateway')}")
    print(f"  ├── {svc_label('auth')}")
    print(f"  └── {svc_label('payment')}")
    print(f"        ├── {svc_label('inventory')}")
    print(f"        └── {svc_label('notification')}")

    incidents = cascade_eng.incidents.active_incidents()
    if incidents:
        print()
        print(f"{C.BOLD}  Active Incidents:{C.RESET}")
        for inc in incidents[:4]:
            sev_col = C.RED if inc["severity"] == "high" else C.YELLOW
            age = int(time.time() - inc["started_at"])
            print(f"  {sev_col}[{inc['severity'].upper()}]{C.RESET} "
                  f"{inc['service']:<14} {inc['type']:<15} "
                  f"{C.GREY}({age}s ago){C.RESET}")

    storms = retry_storm_detector.storming_services()
    if storms:
        print()
        print(f"{C.BOLD}  {C.MAGENTA}⚡ Retry Storms:{C.RESET}")
        for s in storms:
            print(f"  {C.MAGENTA}{s['service']:<14} "
                  f"{s['retry_rate']:.1f} retries/s  "
                  f"amp={s['amplification']:.1f}x  "
                  f"dur={s['duration_s']:.0f}s{C.RESET}")

    scale_stats = scaler.stats()
    if scale_stats["total_scale_ups"] + scale_stats["total_scale_downs"] > 0:
        print()
        print(f"{C.BOLD}  Autoscaler:{C.RESET}  "
              f"{C.CYAN}↑{scale_stats['total_scale_ups']} scale-ups{C.RESET}  "
              f"{C.GREY}↓{scale_stats['total_scale_downs']} scale-downs{C.RESET}")
        for ev in scale_stats["recent_events"][-3:]:
            arrow = "↑" if ev["direction"] == "UP" else "↓"
            col   = C.CYAN if ev["direction"] == "UP" else C.GREY
            print(f"  {col}{arrow} {ev['service']:<14} "
                  f"{ev['old_replicas']}→{ev['new_replicas']} replicas  "
                  f"{ev['reason'][:45]}{C.RESET}")

    print()
    print(f"{C.BOLD}  Live Event Feed:{C.RESET}")
    print(f"  {'─'*62}")
    color_map = {
        "green":   C.GREEN,
        "red":     C.RED,
        "yellow":  C.YELLOW,
        "magenta": C.MAGENTA,
        "cyan":    C.CYAN,
        "white":   C.WHITE,
    }
    LOG_LINES = 10
    for entry in event_log[-LOG_LINES:]:
        col = color_map.get(entry["color"], C.WHITE)
        print(f"  {C.GREY}{entry['time']}{C.RESET}  {col}{entry['message']}{C.RESET}")
    for _ in range(LOG_LINES - min(len(event_log), LOG_LINES)):
        print()

    print(f"\n  {C.GREY}Ctrl+C to stop  ·  Burst traffic every 20s  ·  Watch for circuit breaker trips{C.RESET}")


def _qbar(depth: int, max_size: int, w: int = 12) -> str:
    ratio  = min(depth / max(max_size, 1), 1.0)
    filled = int(ratio * w)
    col    = C.GREEN if ratio < 0.5 else (C.YELLOW if ratio < 0.8 else C.RED)
    return f"{col}{'█'*filled}{'░'*(w-filled)}{C.RESET} {depth:>4}"


def _ecol(rate: float) -> str:
    return C.GREEN if rate < 0.05 else (C.YELLOW if rate < 0.2 else C.RED)



async def run_traffic_generator(bus: EventBus, store: StateStore, stats: dict, event_log: list):
    gen = TrafficGenerator(
        use_http  = False,
        bus       = bus,
        store     = store,
        event_log = event_log,
    )

    original_send = gen._send_direct

    async def patched_send():
        before = gen.total_accepted + gen.total_rejected
        await original_send()
        after = gen.total_accepted + gen.total_rejected
        stats["total_requests"] = gen.total_accepted
        stats["total_errors"]   = gen.total_rejected
        stats["current_mode"]   = gen.current_mode

    gen._send_direct = patched_send
    await gen.run(demo_pattern())



async def dashboard_loop(store: StateStore, cb_reg: CircuitBreakerRegistry,
                         cascade_eng: CascadeEngine, scaler: Autoscaler,
                         stats: dict, event_log: list):
    while True:
        render(store, cb_reg, cascade_eng, scaler, stats, event_log)
        await asyncio.sleep(1.0)



async def main():
    bus         = EventBus()
    store       = StateStore()
    cb_reg      = CircuitBreakerRegistry()
    cascade_eng = CascadeEngine()
    event_log   = []

    stats = {
        "start_time":     time.time(),
        "total_requests": 0,
        "total_errors":   0,
        "current_mode":   "starting...",
    }

    for name, cfg in SERVICE_CONFIGS.items():
        bus.register(name, cfg)
        store.register(name, replicas=cfg.replicas)
        cb = cb_reg.register(
            name,
            failure_threshold = 0.5,
            window_size       = 20,
            open_timeout_s    = 8.0,
        )
        cb.on_open(cascade_eng.on_circuit_open)
        cb.on_close(cascade_eng.on_circuit_close)

    scaler = Autoscaler(store=store, bus=bus, event_log=event_log)

    from simulator.service_runner import _log
    _log(event_log, "Stage 4 simulation started", "cyan")
    _log(event_log, "Watch: retry storms · autoscaler · cascade failures", "cyan")

    try:
        await asyncio.gather(
            [
                run_service(name, bus, store, cb_reg, cascade_eng, event_log)
                for name in SERVICE_ORDER
            ],
            run_traffic_generator(bus, store, stats, event_log),
            scaler.run(),
            dashboard_loop(store, cb_reg, cascade_eng, scaler, stats, event_log),
        )
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    print("\033[?25l", end="")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    finally:
        print("\033[?25h", end="")
        print(f"\n{C.CYAN}Simulation stopped.{C.RESET}\n")
