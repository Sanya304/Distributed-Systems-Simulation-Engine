"""FastAPI application entry point for the API gateway."""

import asyncio
import sys
import logging
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

sys.path.insert(0, "/Users/skumari26/project")

from shared.event_bus import EventBus
from shared.state_store import StateStore
from shared.config import SERVICE_CONFIGS, SERVICE_ORDER

from gateway.middleware import TraceMiddleware, RequestLoggingMiddleware
from gateway.router import router
from gateway.rate_limiter import RateLimiter

from simulator.circuit_breaker import CircuitBreakerRegistry
from simulator.failure_engine import CascadeEngine
from simulator.service_runner import run_service
from simulator.autoscaler import Autoscaler


logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s  %(name)-25s  %(levelname)-8s  %(message)s",
    datefmt = "%H:%M:%S",
)
logger = logging.getLogger("gateway.main")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Gateway starting up...")

    bus         = EventBus()
    store       = StateStore()
    cb_registry = CircuitBreakerRegistry()
    cascade     = CascadeEngine()
    limiter     = RateLimiter(requests_per_second=50.0, burst=100.0)
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

    logger.info(f"Registered services: {SERVICE_ORDER}")

    app.state.bus         = bus
    app.state.store       = store
    app.state.cb_registry = cb_registry
    app.state.cascade     = cascade
    app.state.limiter     = limiter
    app.state.event_log   = event_log

    worker_tasks = []
    for name in SERVICE_ORDER:
        task = asyncio.create_task(
            run_service(name, bus, store, cb_registry, cascade, event_log),
            name=f"worker-{name}",
        )
        worker_tasks.append(task)
        logger.info(f"  Started worker: {name}")

    scaler = Autoscaler(store=store, bus=bus, event_log=event_log)
    autoscaler_task = asyncio.create_task(scaler.run(), name="autoscaler")
    worker_tasks.append(autoscaler_task)
    app.state.autoscaler = scaler

    app.state.worker_tasks = worker_tasks
    logger.info("Gateway ready.")

    yield

    logger.info("Shutting down workers...")
    for task in worker_tasks:
        task.cancel()
    await asyncio.gather(*worker_tasks, return_exceptions=True)
    logger.info("Shutdown complete.")


app = FastAPI(
    title    = "Simulation Engine Gateway",
    version  = "1.0.0",
    lifespan = lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins  = ["*"],
    allow_methods  = ["*"],
    allow_headers  = ["*"],
)
app.add_middleware(TraceMiddleware)
app.add_middleware(RequestLoggingMiddleware)

app.include_router(router)


if __name__ == "__main__":
    uvicorn.run("gateway.main:app", host="0.0.0.0", port=8000, log_level="info")
