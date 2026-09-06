"""Route handlers for the API gateway."""

import time
import uuid
import random
from fastapi import APIRouter, Request, HTTPException, Depends
from fastapi.responses import JSONResponse

from shared.models import Event, EventType
from shared.config import SERVICE_CONFIGS


router = APIRouter()


def get_bus(request: Request):
    return request.app.state.bus

def get_store(request: Request):
    return request.app.state.store

def get_limiter(request: Request):
    return request.app.state.limiter


async def check_rate_limit(request: Request):
    limiter   = get_limiter(request)
    client_ip = request.client.host if request.client else "127.0.0.1"
    if not limiter.is_allowed(client_ip):
        raise HTTPException(
            status_code = 429,
            detail      = {
                "error":   "rate_limit_exceeded",
                "message": f"Too many requests. Limit: {limiter.rps} req/s",
                "limit":   limiter.rps,
            },
            headers={"Retry-After": "1"},
        )



async def _publish_event(
    request:        Request,
    target_service: str,
    event_type:     EventType = EventType.REQUEST,
    retry_count:    int       = 0,
) -> dict:
    bus   = get_bus(request)
    store = get_store(request)

    trace_id     = getattr(request.state, "trace_id",   str(uuid.uuid4()))
    request_id   = getattr(request.state, "request_id", str(uuid.uuid4()))
    payload_size = int(request.headers.get("Content-Length", 256))

    event = Event(
        source_service = "gateway",
        target_service = target_service,
        event_type     = event_type,
        trace_id       = trace_id,
        request_id     = request_id,
        timestamp      = time.time(),
        region         = request.headers.get("X-Region", "us-east"),
        retry_count    = retry_count,
        payload_size   = payload_size,
        metadata       = {
            "method":    request.method,
            "path":      str(request.url.path),
            "client_ip": request.client.host if request.client else "unknown",
        },
    )

    gateway_state = store.get_sync("gateway")
    if gateway_state and not gateway_state.is_alive:
        raise HTTPException(
            status_code = 503,
            detail      = {"error": "gateway_unavailable", "trace_id": trace_id},
        )

    accepted = await bus.publish(target_service, event)
    if not accepted:
        queue_depth = bus.queue_depth(target_service)
        max_queue   = SERVICE_CONFIGS[target_service].max_queue_size
        raise HTTPException(
            status_code = 503,
            detail      = {
                "error":       "service_queue_full",
                "service":     target_service,
                "queue_depth": queue_depth,
                "max_queue":   max_queue,
                "trace_id":    trace_id,
                "message":     f"{target_service} is overloaded. Try again later.",
            },
        )

    return {
        "status":      "accepted",
        "trace_id":    trace_id,
        "request_id":  request_id,
        "service":     target_service,
        "queue_depth": bus.queue_depth(target_service),
        "timestamp":   event.timestamp,
    }



@router.post("/request", dependencies=[Depends(check_rate_limit)])
async def generic_request(request: Request):
    return JSONResponse(
        content     = await _publish_event(request, "gateway"),
        status_code = 202,
    )


@router.post("/auth/{path:path}", dependencies=[Depends(check_rate_limit)])
async def auth_request(request: Request, path: str):
    return JSONResponse(
        content     = await _publish_event(request, "auth"),
        status_code = 202,
    )


@router.post("/payment/{path:path}", dependencies=[Depends(check_rate_limit)])
async def payment_request(request: Request, path: str):
    return JSONResponse(
        content     = await _publish_event(request, "payment"),
        status_code = 202,
    )


@router.post("/inventory/{path:path}", dependencies=[Depends(check_rate_limit)])
async def inventory_request(request: Request, path: str):
    return JSONResponse(
        content     = await _publish_event(request, "inventory"),
        status_code = 202,
    )


@router.post("/notification/{path:path}", dependencies=[Depends(check_rate_limit)])
async def notification_request(request: Request, path: str):
    return JSONResponse(
        content     = await _publish_event(request, "notification"),
        status_code = 202,
    )


@router.get("/status")
async def service_status(request: Request):
    bus     = get_bus(request)
    store   = get_store(request)
    limiter = get_limiter(request)

    status = {}
    for name in SERVICE_CONFIGS:
        state = store.get_sync(name)
        if state:
            status[name] = {
                "circuit_state":  state.circuit_state.value,
                "queue_depth":    bus.queue_depth(name),
                "avg_latency":    round(state.avg_latency, 2),
                "error_rate":     round(state.error_rate, 3),
                "cpu_usage":      round(state.cpu_usage, 3),
                "is_alive":       state.is_alive,
                "total_requests": state.total_requests,
                "total_errors":   state.total_errors,
            }
    return {"services": status, "rate_limiter": limiter.stats()}


@router.get("/health")
async def health(request: Request):
    bus = get_bus(request)
    registered = bus.registered_services()
    return {
        "status":   "healthy",
        "services": registered,
        "queues":   {name: bus.queue_depth(name) for name in registered},
    }
