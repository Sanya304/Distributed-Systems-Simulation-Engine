"""Stage 1 verification — tests every component in the shared/ package."""

import asyncio
import sys

sys.path.insert(0, "/Users/skumari26/project")

from shared.models import Event, EventType, CircuitState, ServiceConfig
from shared.event_bus import EventBus           # import class directly to create isolated instances per test
from shared.state_store import StateStore       # same — isolated instances, not the singletons
from shared.config import SERVICE_CONFIGS, DEPENDENCY_GRAPH, SERVICE_ORDER



async def test_models():
    print("\n--- Test 1: Models ---")

    event = Event(
        source_service="gateway",
        target_service="payment",
        event_type=EventType.REQUEST,
        region="us-east",
        payload_size=512,
    )

    assert event.trace_id != "", "trace_id should be auto-generated"
    assert event.request_id != "", "request_id should be auto-generated"
    assert event.trace_id != event.request_id, "trace_id and request_id should differ"
    assert event.retry_count == 0, "new event should have retry_count=0"

    print(f"  Event created:    trace_id={event.trace_id[:8]}...  type={event.event_type.value}")

    d = event.to_dict()
    assert d["source_service"] == "gateway"
    assert d["target_service"] == "payment"
    assert d["event_type"] == "request"     # should be the .value string, not the enum
    assert d["retry_count"] == 0
    print(f"  to_dict() works:  source={d['source_service']} → target={d['target_service']}")

    retry_event = Event(
        source_service="gateway",
        target_service="payment",
        event_type=EventType.RETRY,
        trace_id=event.trace_id,        retry_count=1,
    )
    assert retry_event.trace_id == event.trace_id, "retry should share trace_id with original"
    assert retry_event.request_id != event.request_id, "retry gets a new request_id"
    print(f"  Retry event:      same trace_id={retry_event.trace_id[:8]}... retry_count=1")

    print("  PASS")



async def test_event_bus():
    print("\n--- Test 2: EventBus publish/consume ---")

    local_bus = EventBus()
    config = SERVICE_CONFIGS["payment"]
    local_bus.register("payment", config)

    event = Event(
        source_service="gateway",
        target_service="payment",
        event_type=EventType.REQUEST,
    )

    published = await local_bus.publish("payment", event)
    assert published is True, "publish to empty queue should succeed"
    print(f"  Publish to empty queue: {published} (expected True)")

    depth = local_bus.queue_depth("payment")
    assert depth == 1, f"queue depth should be 1, got {depth}"
    print(f"  Queue depth after publish: {depth} (expected 1)")

    received = await local_bus.consume("payment")
    assert received.trace_id == event.trace_id, "consumed event should match published event"
    print(f"  Consumed event: trace_id={received.trace_id[:8]}... (matches published)")

    depth = local_bus.queue_depth("payment")
    assert depth == 0, f"queue depth after consume should be 0, got {depth}"
    print(f"  Queue depth after consume: {depth} (expected 0)")

    ok = await local_bus.publish("nonexistent_service", event)
    assert ok is False, "publishing to unknown service should return False"
    print(f"  Publish to unknown service: {ok} (expected False, no crash)")

    print("  PASS")



async def test_queue_backpressure():
    print("\n--- Test 3: Queue Backpressure (maxsize enforcement) ---")

    small_config = ServiceConfig(
        name="small_test",
        base_latency_ms=10,
        failure_probability=0.0,
        max_queue_size=3,
        timeout_ms=1000,
        dependencies=[],
    )
    local_bus = EventBus()
    local_bus.register("small_test", small_config)

    results = []
    for i in range(5):
        e = Event(
            source_service="gateway",
            target_service="small_test",
            event_type=EventType.REQUEST,
        )
        ok = await local_bus.publish("small_test", e)
        results.append(ok)

    accepted = sum(results)     # count of True values
    rejected = len(results) - accepted

    print(f"  Sent 5 events to queue of size 3:")
    print(f"    Accepted: {accepted} (expected 3)")
    print(f"    Rejected: {rejected} (expected 2)")

    assert accepted == 3, f"should accept exactly 3, got {accepted}"
    assert rejected == 2, f"should reject exactly 2, got {rejected}"

    assert local_bus.is_full("small_test") is True
    print(f"  is_full() after saturation: True (correct)")

    await local_bus.consume("small_test")
    assert local_bus.is_full("small_test") is False
    new_event = Event(source_service="gateway", target_service="small_test", event_type=EventType.REQUEST)
    ok = await local_bus.publish("small_test", new_event)
    assert ok is True, "should accept after consuming one slot"
    print(f"  After consuming 1, next publish accepted: {ok} (expected True)")

    print("  PASS")



async def test_state_store():
    print("\n--- Test 4: StateStore ---")

    local_store = StateStore()

    for name, cfg in SERVICE_CONFIGS.items():
        local_store.register(name, replicas=cfg.replicas)

    state = await local_store.get("payment")
    assert state.name == "payment"
    assert state.active_requests == 0
    assert state.queue_depth == 0
    assert state.error_rate == 0.0
    assert state.circuit_state == CircuitState.CLOSED
    assert state.is_alive is True
    print(f"  Initial payment state: active={state.active_requests}, "
          f"circuit={state.circuit_state.value}, alive={state.is_alive}")

    await local_store.update("payment",
        active_requests=5,
        cpu_usage=0.65,
        avg_latency=145.2,
    )
    state = await local_store.get("payment")
    assert state.active_requests == 5
    assert state.cpu_usage == 0.65
    assert abs(state.avg_latency - 145.2) < 0.001
    print(f"  After update: active={state.active_requests}, "
          f"cpu={state.cpu_usage}, latency={state.avg_latency}ms")

    for _ in range(10):
        await local_store.increment("payment", "total_requests")
    for _ in range(2):
        await local_store.increment("payment", "total_errors")

    state = await local_store.get("payment")
    assert state.total_requests == 10
    assert state.total_errors == 2
    print(f"  After increments: total_requests={state.total_requests}, "
          f"total_errors={state.total_errors}")

    await local_store.update("payment", circuit_state=CircuitState.OPEN)
    state = await local_store.get("payment")
    assert state.circuit_state == CircuitState.OPEN
    print(f"  Circuit breaker opened: {state.circuit_state.value}")

    snapshot = await local_store.snapshot()
    assert len(snapshot) == 5, f"snapshot should have 5 services, got {len(snapshot)}"
    assert "gateway" in snapshot
    assert "payment" in snapshot
    assert "inventory" in snapshot
    assert snapshot["payment"].active_requests == 5
    print(f"  Snapshot: {len(snapshot)} services, payment.active_requests={snapshot['payment'].active_requests}")

    state_copy = await local_store.get("auth")
    state_copy.active_requests = 999
    original = await local_store.get("auth")
    assert original.active_requests != 999, "modifying copy should not affect store"
    print(f"  Copy isolation: modifying copy doesn't change store (original={original.active_requests})")

    try:
        await local_store.get("nonexistent")
        assert False, "should have raised KeyError"
    except KeyError:
        print(f"  Unknown service raises KeyError (correct)")

    print("  PASS")



async def test_config():
    print("\n--- Test 5: Service Configuration ---")

    assert len(SERVICE_CONFIGS) == 5
    print(f"  Service count: {len(SERVICE_CONFIGS)}")

    print(f"\n  {'Service':<14} {'Latency':>8} {'Fail%':>6} {'Queue':>6} {'Replicas':>9}")
    print(f"  {'-'*14} {'-'*8} {'-'*6} {'-'*6} {'-'*9}")
    for name, cfg in SERVICE_CONFIGS.items():
        print(f"  {name:<14} {cfg.base_latency_ms:>6.0f}ms "
              f"{cfg.failure_probability:>5.0%}  "
              f"{cfg.max_queue_size:>6}  "
              f"{cfg.replicas:>9}")

    assert set(DEPENDENCY_GRAPH.keys()) == set(SERVICE_CONFIGS.keys()), \
        "DEPENDENCY_GRAPH must have an entry for every service"
    print(f"\n  Dependency graph covers all {len(DEPENDENCY_GRAPH)} services")

    all_service_names = set(SERVICE_CONFIGS.keys())
    for service, deps in DEPENDENCY_GRAPH.items():
        for dep in deps:
            assert dep in all_service_names, \
                f"Unknown service '{dep}' in dependencies of '{service}'"
    print(f"  All dependency targets are valid service names")

    assert "auth" in DEPENDENCY_GRAPH["gateway"]
    assert "payment" in DEPENDENCY_GRAPH["gateway"]
    assert "inventory" in DEPENDENCY_GRAPH["payment"]
    assert "notification" in DEPENDENCY_GRAPH["payment"]
    assert DEPENDENCY_GRAPH["inventory"] == []
    assert DEPENDENCY_GRAPH["notification"] == []
    print(f"  Topology validated: gateway→auth, gateway→payment, payment→inventory/notification")

    assert sorted(SERVICE_ORDER) == sorted(SERVICE_CONFIGS.keys()), \
        "SERVICE_ORDER must contain all services"
    print(f"  Service order: {' → '.join(SERVICE_ORDER)}")

    print("  PASS")



async def main():
    print("=" * 55)
    print("  Stage 1 — Core Models, Event Bus & State Store")
    print("=" * 55)

    await test_models()
    await test_event_bus()
    await test_queue_backpressure()
    await test_state_store()
    await test_config()

    print("\n" + "=" * 55)
    print("  All Stage 1 tests passed. Ready for Stage 2.")
    print("=" * 55)


if __name__ == "__main__":
    asyncio.run(main())
