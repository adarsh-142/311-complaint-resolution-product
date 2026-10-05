from __future__ import annotations

import json
import logging
import time
import uuid

from fastapi import FastAPI, Request

logger = logging.getLogger("api.request")


def register_request_middleware(app: FastAPI) -> None:
    @app.middleware("http")
    async def request_context_middleware(request: Request, call_next):
        run_id = request.headers.get("X-Run-ID") or str(uuid.uuid4())
        request.state.run_id = run_id

        start = time.perf_counter()
        logger.info(
            json.dumps(
                {
                    "event": "request_started",
                    "run_id": run_id,
                    "method": request.method,
                    "path": request.url.path,
                }
            )
        )

        response = await call_next(request)

        duration_ms = round((time.perf_counter() - start) * 1000, 2)
        response.headers.setdefault("X-Run-ID", run_id)
        logger.info(
            json.dumps(
                {
                    "event": "request_completed",
                    "run_id": run_id,
                    "method": request.method,
                    "path": request.url.path,
                    "status_code": response.status_code,
                    "duration_ms": duration_ms,
                }
            )
        )

        return response
