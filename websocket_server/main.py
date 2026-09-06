"""FastAPI WebSocket server for real-time metrics."""

import asyncio
import sys
import logging
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent.parent))

from shared.event_bus import EventBus
from shared.state_store import StateStore
from shared.config import SERVICE_CONFIGS, SERVICE_ORDER

from simulator.circuit_breaker import CircuitBreakerRegistry
from simulator.failure_engine import CascadeEngine
from simulator.retry_storm import storm_detector
from simulator.autoscaler import Autoscaler
from simulator.service_runner import run_service

from websocket_server.aggregator import MetricsAggregator
from websocket_server.manager import ConnectionManager

from gateway.rate_limiter import RateLimiter
from gateway.router import router as gateway_router

from traffic_generator.generator import TrafficGenerator
from traffic_generator.patterns import demo_pattern


logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s  %(name)-30s  %(levelname)-8s  %(message)s",
    datefmt = "%H:%M:%S",
)
logger = logging.getLogger("websocket_server.main")

BROADCAST_INTERVAL_S = 1.0


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting full simulation stack...")

    bus         = EventBus()
    store       = StateStore()
    cb_registry = CircuitBreakerRegistry()
    cascade     = CascadeEngine()
    limiter     = RateLimiter(requests_per_second=100.0, burst=200.0)
    event_log   = []

    for name, cfg in SERVICE_CONFIGS.items():
        bus.register(name, cfg)
        store.register(name, replicas=cfg.replicas)
        cb = cb_registry.register(
            name,
            failure_threshold = 0.5,
            window_size       = 20,
            open_timeout_s    = 8.0,
        )
        cb.on_open(cascade.on_circuit_open)
        cb.on_close(cascade.on_circuit_close)

    scaler = Autoscaler(store=store, bus=bus, event_log=event_log)

    aggregator = MetricsAggregator(
        store       = store,
        bus         = bus,
        cb_registry = cb_registry,
        cascade     = cascade,
        storm       = storm_detector,
        scaler      = scaler,
        event_log   = event_log,
    )
    manager = ConnectionManager()

    app.state.bus         = bus
    app.state.store       = store
    app.state.cb_registry = cb_registry
    app.state.cascade     = cascade
    app.state.limiter     = limiter
    app.state.event_log   = event_log
    app.state.scaler      = scaler
    app.state.aggregator  = aggregator
    app.state.manager     = manager

    tasks = []

    for name in SERVICE_ORDER:
        task = asyncio.create_task(
            run_service(name, bus, store, cb_registry, cascade, event_log),
            name=f"worker-{name}",
        )
        tasks.append(task)

    tasks.append(asyncio.create_task(scaler.run(), name="autoscaler"))

    tgen = TrafficGenerator(use_http=False, bus=bus, store=store, event_log=event_log)
    app.state.traffic_gen = tgen
    tasks.append(asyncio.create_task(
        tgen.run(demo_pattern()),
        name="traffic-generator",
    ))

    tasks.append(asyncio.create_task(
        _broadcast_loop(aggregator, manager),
        name="ws-broadcaster",
    ))

    app.state.tasks = tasks
    logger.info(f"All tasks running: {[t.get_name() for t in tasks]}")

    yield   # serve requests

    logger.info("Shutting down...")
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    logger.info("Shutdown complete.")


async def _broadcast_loop(aggregator: MetricsAggregator, manager: ConnectionManager):
    logger.info("WebSocket broadcast loop started.")
    while True:
        try:
            snapshot = aggregator.build_snapshot()
            json_str = aggregator.to_json(snapshot)
            if manager.active_count > 0:
                await manager.broadcast(json_str)
        except Exception as exc:
            logger.error(f"Broadcast error: {exc}", exc_info=True)
        await asyncio.sleep(BROADCAST_INTERVAL_S)



app = FastAPI(
    title    = "Simulation Metrics WebSocket Server",
    version  = "1.0.0",
    lifespan = lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins  = ["*"],
    allow_methods  = ["*"],
    allow_headers  = ["*"],
)


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    manager    = websocket.app.state.manager
    aggregator = websocket.app.state.aggregator

    await manager.connect(websocket)

    try:
        history = aggregator.get_history()
        if history:
            await manager.send_history(websocket, history)
            logger.info(f"Sent {len(history)} history entries to new client")

        while True:
            data = await websocket.receive_text()
            if data:
                try:
                    import json as _json
                    msg = _json.loads(data)
                    if msg.get("type") == "ping":
                        await websocket.send_text('{"type":"pong"}')
                except Exception:
                    pass

    except WebSocketDisconnect:
        manager.disconnect(websocket)



@app.get("/ws/status")
async def ws_status(request: Request):
    return {
        "status":      "healthy",
        "connections": request.app.state.manager.stats(),
        "traffic_gen": request.app.state.traffic_gen.stats(),
        "autoscaler":  request.app.state.scaler.stats(),
    }


@app.get("/ws/snapshot")
async def get_snapshot(request: Request):
    snapshot = request.app.state.aggregator.build_snapshot()
    return snapshot


@app.get("/ws/history")
async def get_history(request: Request):
    return {"history": request.app.state.aggregator.get_history()}


@app.get("/health")
async def health(request: Request):
    bus        = request.app.state.bus
    registered = bus.registered_services()
    return {
        "status":   "healthy",
        "services": registered,
        "queues":   {name: bus.queue_depth(name) for name in registered},
    }



class KillServiceRequest(BaseModel):
    service: str

class AddLatencyRequest(BaseModel):
    service: str
    extra_ms: float

class NetworkPartitionRequest(BaseModel):
    service: str
    latency_ms: float = 2000.0


@app.post("/chaos/kill-service")
async def chaos_kill_service(body: KillServiceRequest, request: Request):
    store = request.app.state.store
    if body.service not in store.all_names():
        raise HTTPException(status_code=404, detail=f"Unknown service: {body.service}")

    await store.update(body.service, is_alive=False)

    event_log = request.app.state.event_log
    event_log.append({
        "time":    __import__("time").strftime("%H:%M:%S"),
        "message": f"CHAOS  ☠ {body.service} KILLED by operator",
        "color":   "red",
    })
    logger.info(f"[CHAOS] Killed service: {body.service}")
    return {"status": "killed", "service": body.service}


@app.post("/chaos/add-latency")
async def chaos_add_latency(body: AddLatencyRequest, request: Request):
    store = request.app.state.store
    if body.service not in store.all_names():
        raise HTTPException(status_code=404, detail=f"Unknown service: {body.service}")

    await store.update(body.service, extra_latency_ms=body.extra_ms)

    event_log = request.app.state.event_log
    event_log.append({
        "time":    __import__("time").strftime("%H:%M:%S"),
        "message": f"CHAOS  ⏱ {body.service} +{body.extra_ms:.0f}ms latency injected",
        "color":   "yellow",
    })
    logger.info(f"[CHAOS] Added {body.extra_ms}ms latency to: {body.service}")
    return {"status": "latency_added", "service": body.service, "extra_ms": body.extra_ms}


@app.post("/chaos/network-partition")
async def chaos_network_partition(body: NetworkPartitionRequest, request: Request):
    store = request.app.state.store
    if body.service not in store.all_names():
        raise HTTPException(status_code=404, detail=f"Unknown service: {body.service}")

    await store.update(body.service, is_alive=False, extra_latency_ms=body.latency_ms)

    event_log = request.app.state.event_log
    event_log.append({
        "time":    __import__("time").strftime("%H:%M:%S"),
        "message": f"CHAOS  ⚡ {body.service} NETWORK PARTITION (+{body.latency_ms:.0f}ms, killed)",
        "color":   "red",
    })
    logger.info(f"[CHAOS] Network partition on: {body.service} ({body.latency_ms}ms)")
    return {
        "status":     "partitioned",
        "service":    body.service,
        "latency_ms": body.latency_ms,
    }


@app.post("/chaos/heal-all")
async def chaos_heal_all(request: Request):
    store     = request.app.state.store
    healed    = []

    for name in store.all_names():
        state = store.get_sync(name)
        if state and (not state.is_alive or state.extra_latency_ms > 0):
            await store.update(name, is_alive=True, extra_latency_ms=0.0)
            healed.append(name)

    event_log = request.app.state.event_log
    event_log.append({
        "time":    __import__("time").strftime("%H:%M:%S"),
        "message": f"CHAOS  ✓ ALL SERVICES HEALED: {', '.join(healed) if healed else 'nothing to heal'}",
        "color":   "green",
    })
    logger.info(f"[CHAOS] Healed all services: {healed}")
    return {"status": "healed", "services": healed}


app.include_router(gateway_router)



if __name__ == "__main__":
    uvicorn.run(
        "websocket_server.main:app",
        host      = "0.0.0.0",
        port      = 8001,
        log_level = "info",
    )
