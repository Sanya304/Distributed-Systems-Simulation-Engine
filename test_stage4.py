"""Stage 4 verification — retry storm detection, backoff, autoscaler."""

import asyncio
import sys
import time
sys.path.insert(0, "/Users/skumari26/project")

from simulator.retry_storm import (
    RetryStormDetector, BackoffConfig, compute_backoff, storm_detector
)
from simulator.autoscaler import Autoscaler, POLL_INTERVAL_S
from shared.event_bus import EventBus
from shared.state_store import StateStore
from shared.config import SERVICE_CONFIGS, SERVICE_ORDER


async def test_backoff():
    print("\n--- Test 1: Exponential Backoff Calculator ---")

    cfg = BackoffConfig(
        base_delay_s  = 0.1,
        max_delay_s   = 10.0,
        multiplier    = 2.0,
        jitter_factor = 0.0,   # zero jitter for deterministic test
    )

    delays = [compute_backoff(i, cfg) for i in range(5)]

    assert abs(delays[0] - 0.1) < 0.001, f"retry 0 should be 0.1s, got {delays[0]}"
    assert abs(delays[1] - 0.2) < 0.001, f"retry 1 should be 0.2s, got {delays[1]}"
    assert abs(delays[2] - 0.4) < 0.001, f"retry 2 should be 0.4s, got {delays[2]}"
    assert abs(delays[3] - 0.8) < 0.001, f"retry 3 should be 0.8s, got {delays[3]}"
    assert abs(delays[4] - 1.6) < 0.001, f"retry 4 should be 1.6s, got {delays[4]}"
    print(f"  Delays: {[f'{d:.2f}s' for d in delays]}")
    print(f"  Each doubles correctly: {all(abs(delays[i+1]/delays[i] - 2.0) < 0.01 for i in range(4))}")

    big_delay = compute_backoff(20, cfg)
    assert big_delay == 10.0, f"Should be capped at 10s, got {big_delay}"
    print(f"  Max cap: retry 20 → {big_delay}s (capped at 10s)")

    jitter_cfg = BackoffConfig(base_delay_s=0.1, max_delay_s=10.0,
                               multiplier=2.0, jitter_factor=0.5)
    jitter_delay = compute_backoff(0, jitter_cfg)
    assert jitter_delay >= 0.1, "Jitter should not reduce below base"
    assert jitter_delay <= 0.15, f"With 50% jitter on 0.1s: max is 0.15s, got {jitter_delay:.3f}s"
    print(f"  With 50% jitter: retry 0 → {jitter_delay:.3f}s (range: 0.100-0.150s)")

    print("  PASS")


async def test_retry_storm_detector():
    print("\n--- Test 2: Retry Storm Detector ---")

    detector = RetryStormDetector(storm_threshold_rps=3.0, window_s=5.0)

    assert detector.is_storming("payment") is False
    assert detector.retry_rate("payment") == 0.0
    print(f"  No retries: is_storming=False, rate=0.0")

    for _ in range(20):
        detector.record_request("payment")

    for _ in range(5):
        detector.record_retry("payment")

    rate = detector.retry_rate("payment")
    print(f"  After 5 retries in 5s window: rate={rate:.2f}/s (threshold=3.0)")

    for _ in range(20):
        detector.record_retry("payment")

    rate = detector.retry_rate("payment")
    assert rate >= 3.0, f"Should be storming: rate={rate:.2f}"
    assert detector.is_storming("payment") is True
    print(f"  After 25 retries in 5s window: rate={rate:.2f}/s → STORM DETECTED")

    amp = detector.amplification_factor("payment")
    assert amp > 1.0, f"Amplification should be > 1.0, got {amp}"
    print(f"  Amplification factor: {amp:.2f}x (20 originals + 25 retries)")

    storms = detector.storming_services()
    assert any(s["service"] == "payment" for s in storms)
    storm = next(s for s in storms if s["service"] == "payment")
    print(f"  storming_services(): {storm}")

    assert detector.is_storming("auth") is False
    print(f"  auth (no retries): is_storming=False")

    fast_detector = RetryStormDetector(storm_threshold_rps=2.0, window_s=0.2)
    for _ in range(10):
        fast_detector.record_retry("inventory")
    assert fast_detector.is_storming("inventory") is True
    await asyncio.sleep(0.25)  # wait for window to expire
    assert fast_detector.is_storming("inventory") is False
    print(f"  After window expiry (0.2s): storm cleared")

    print("  PASS")


# ---------------------------------------------------------------------------
# Test 3: Autoscaler scale-up decision
# ---------------------------------------------------------------------------

async def test_autoscaler_scale_up():
    print("\n--- Test 3: Autoscaler — Scale Up ---")

    local_bus   = EventBus()
    local_store = StateStore()
    event_log   = []

    for name, cfg in SERVICE_CONFIGS.items():
        local_bus.register(name, cfg)
        local_store.register(name, replicas=cfg.replicas)

    scaler = Autoscaler(store=local_store, bus=local_bus, event_log=event_log)

    # Simulate payment being overloaded
    # cpu_threshold for payment = 0.7, so cpu=0.85 should trigger scale-up
    await local_store.update("payment",
        cpu_usage       = 0.85,   # above threshold (0.7)
        active_requests = 25,
    )

    initial_replicas = local_store.get_sync("payment").replicas
    print(f"  Initial payment replicas: {initial_replicas}")

    # Force scale-up cooldown to 0 so test doesn't wait 10s
    scaler._scaler_states["payment"].last_scale_up_time = 0.0

    # Run one evaluation cycle
    await scaler._evaluate_service("payment")

    new_replicas = local_store.get_sync("payment").replicas
    assert new_replicas > initial_replicas, \
        f"Should have scaled up: {initial_replicas} → {new_replicas}"
    print(f"  After cpu=85% (threshold=70%): replicas {initial_replicas} → {new_replicas}")

    # Check event log
    assert len(event_log) > 0, "Scale-up should log an event"
    last_log = event_log[-1]["message"]
    assert "SCALE" in last_log and "↑" in last_log
    print(f"  Event log: {last_log}")

    assert scaler.total_scale_ups == 1
    print(f"  total_scale_ups: {scaler.total_scale_ups}")

    print("  PASS")


# ---------------------------------------------------------------------------
# Test 4: Autoscaler scale-down decision (with stability window)
# ---------------------------------------------------------------------------

async def test_autoscaler_scale_down():
    print("\n--- Test 4: Autoscaler — Scale Down (stability window) ---")

    local_bus   = EventBus()
    local_store = StateStore()
    event_log   = []

    for name, cfg in SERVICE_CONFIGS.items():
        local_bus.register(name, cfg)
        local_store.register(name, replicas=cfg.replicas)

    scaler = Autoscaler(store=local_store, bus=local_bus, event_log=event_log)

    # Set payment to 3 replicas (it starts at 3 from SERVICE_CONFIGS)
    # and simulate very low load
    await local_store.update("payment",
        replicas        = 3,
        cpu_usage       = 0.05,   # well below threshold * 0.3 (0.7 * 0.3 = 0.21)
        active_requests = 1,
    )

    # Force scale-down cooldown to 0
    scaler._scaler_states["payment"].last_scale_down_time = 0.0

    # First 2 evaluations: stable_below_checks increments but doesn't trigger yet
    # (SCALE_DOWN_STABILITY_CHECKS = 3)
    for i in range(2):
        await scaler._evaluate_service("payment")
        checks = scaler._scaler_states["payment"].stable_below_checks
        print(f"  Eval {i+1}: stable_below_checks={checks} (need 3 to trigger)")

    assert local_store.get_sync("payment").replicas == 3, \
        "Should NOT have scaled down yet (stability window not met)"

    # 3rd evaluation: stability window met → scale down
    await scaler._evaluate_service("payment")

    new_replicas = local_store.get_sync("payment").replicas
    assert new_replicas < 3, f"Should have scaled down after 3 stable polls, got {new_replicas}"
    print(f"  After 3 stable polls: replicas 3 → {new_replicas}")

    last_log = event_log[-1]["message"]
    assert "SCALE" in last_log and "↓" in last_log
    print(f"  Event log: {last_log}")

    assert scaler.total_scale_downs == 1
    print(f"  total_scale_downs: {scaler.total_scale_downs}")

    print("  PASS")


# ---------------------------------------------------------------------------
# Test 5: Autoscaler respects MIN/MAX limits
# ---------------------------------------------------------------------------

async def test_autoscaler_limits():
    print("\n--- Test 5: Autoscaler — Min/Max Replica Limits ---")

    from simulator.autoscaler import MIN_REPLICAS, MAX_REPLICAS

    local_bus   = EventBus()
    local_store = StateStore()
    event_log   = []

    for name, cfg in SERVICE_CONFIGS.items():
        local_bus.register(name, cfg)
        local_store.register(name, replicas=cfg.replicas)

    scaler = Autoscaler(store=local_store, bus=local_bus, event_log=event_log)

    # ── Can't scale below MIN_REPLICAS ────────────────────────────────────
    await local_store.update("notification",
        replicas        = MIN_REPLICAS,  # already at minimum
        cpu_usage       = 0.01,
        active_requests = 0,
    )
    scaler._scaler_states["notification"].last_scale_down_time = 0.0
    scaler._scaler_states["notification"].stable_below_checks  = 10  # already stable

    await scaler._evaluate_service("notification")
    assert local_store.get_sync("notification").replicas == MIN_REPLICAS, \
        f"Should stay at MIN_REPLICAS={MIN_REPLICAS}"
    print(f"  At MIN_REPLICAS={MIN_REPLICAS}: no further scale-down")

    # ── Can't scale above MAX_REPLICAS ────────────────────────────────────
    await local_store.update("notification",
        replicas        = MAX_REPLICAS,  # already at maximum
        cpu_usage       = 0.99,          # very overloaded
        active_requests = 100,
    )
    scaler._scaler_states["notification"].last_scale_up_time = 0.0

    await scaler._evaluate_service("notification")
    assert local_store.get_sync("notification").replicas == MAX_REPLICAS, \
        f"Should stay at MAX_REPLICAS={MAX_REPLICAS}"
    print(f"  At MAX_REPLICAS={MAX_REPLICAS}: no further scale-up")

    print("  PASS")


# ---------------------------------------------------------------------------
# Test 6: End-to-end — autoscaler running alongside service workers
# ---------------------------------------------------------------------------

async def test_autoscaler_integration():
    print("\n--- Test 6: End-to-End Autoscaler Integration ---")

    from simulator.circuit_breaker import CircuitBreakerRegistry
    from simulator.failure_engine import CascadeEngine
    from simulator.service_runner import run_service

    local_bus   = EventBus()
    local_store = StateStore()
    cb_reg      = CircuitBreakerRegistry()
    cascade     = CascadeEngine()
    event_log   = []

    for name, cfg in SERVICE_CONFIGS.items():
        local_bus.register(name, cfg)
        local_store.register(name, replicas=cfg.replicas)
        cb = cb_reg.register(name, failure_threshold=0.5, window_size=20, open_timeout_s=5.0)
        cb.on_open(cascade.on_circuit_open)
        cb.on_close(cascade.on_circuit_close)

    scaler = Autoscaler(store=local_store, bus=local_bus, event_log=event_log)

    # Manually overload payment so autoscaler can observe it
    await local_store.update("payment", cpu_usage=0.95, active_requests=30)
    scaler._scaler_states["payment"].last_scale_up_time = 0.0

    initial = local_store.get_sync("payment").replicas
    print(f"  Initial payment replicas: {initial}")

    # Run autoscaler for one poll cycle
    await scaler._evaluate_service("payment")

    after = local_store.get_sync("payment").replicas
    print(f"  After overload (cpu=95%): replicas {initial} → {after}")
    assert after > initial, "Autoscaler should have scaled up"

    # Check stats
    stats = scaler.stats()
    assert stats["total_scale_ups"] >= 1
    assert "payment" in stats["current_replicas"]
    print(f"  Stats: {stats['total_scale_ups']} scale-ups, "
          f"current replicas: {stats['current_replicas']}")

    print("  PASS")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main():
    print("=" * 60)
    print("  Stage 4 — Retry Storm, Backoff, Autoscaler")
    print("=" * 60)

    await test_backoff()
    await test_retry_storm_detector()
    await test_autoscaler_scale_up()
    await test_autoscaler_scale_down()
    await test_autoscaler_limits()
    await test_autoscaler_integration()

    print("\n" + "=" * 60)
    print("  All Stage 4 tests passed. Ready for Stage 4 demo.")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
