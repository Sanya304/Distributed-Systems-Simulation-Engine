"""Request middleware for the API gateway."""

import time
import uuid
import logging
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware


logger = logging.getLogger("gateway.middleware")


class TraceMiddleware(BaseHTTPMiddleware):

    async def dispatch(self, request: Request, call_next) -> Response:
        start_time = time.monotonic()

        trace_id = request.headers.get("X-Trace-Id") or str(uuid.uuid4())

        request_id = str(uuid.uuid4())

        request.state.trace_id  = trace_id
        request.state.request_id = request_id
        request.state.start_time = start_time

        try:
            response = await call_next(request)
        except Exception as exc:
            logger.error(
                f"Unhandled exception | trace={trace_id} | error={exc}",
                exc_info=True,
            )
            return Response(
                content=f'{{"error": "internal_error", "trace_id": "{trace_id}"}}',
                status_code=500,
                media_type="application/json",
                headers={"X-Trace-Id": trace_id},
            )

        elapsed_ms = (time.monotonic() - start_time) * 1000

        response.headers["X-Trace-Id"]      = trace_id
        response.headers["X-Request-Id"]    = request_id
        response.headers["X-Response-Time"] = f"{elapsed_ms:.2f}ms"
        response.headers["X-Gateway"]       = "simulation-gateway/1.0"

        logger.debug(
            f"{request.method} {request.url.path} "
            f"→ {response.status_code} "
            f"({elapsed_ms:.1f}ms) "
            f"trace={trace_id[:8]}..."
        )

        return response


class RequestLoggingMiddleware(BaseHTTPMiddleware):

    SKIP_PATHS = {"/health", "/metrics", "/favicon.ico"}

    async def dispatch(self, request: Request, call_next) -> Response:
        if request.url.path not in self.SKIP_PATHS:
            client_ip = request.client.host if request.client else "unknown"
            logger.info(
                f"→ {request.method} {request.url.path} "
                f"from {client_ip}"
            )
        return await call_next(request)
