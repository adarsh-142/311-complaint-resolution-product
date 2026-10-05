from __future__ import annotations

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from api.schemas import ErrorBody, ErrorResponse


def _build_error_response(
    *,
    status_code: int,
    code: str,
    message: str,
    details: dict | None = None,
    run_id: str | None = None,
) -> JSONResponse:
    payload = ErrorResponse(
        error=ErrorBody(
            code=code,
            message=message,
            details=details,
            run_id=run_id,
        )
    )
    response = JSONResponse(status_code=status_code, content=payload.model_dump())
    if run_id:
        response.headers["X-Run-ID"] = run_id
    return response


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(RequestValidationError)
    async def request_validation_handler(request: Request, exc: RequestValidationError):
        return _build_error_response(
            status_code=422,
            code="request_validation_error",
            message="Request validation failed",
            details={"errors": exc.errors()},
            run_id=getattr(request.state, "run_id", None),
        )

    @app.exception_handler(HTTPException)
    async def http_exception_handler(request: Request, exc: HTTPException):
        if isinstance(exc.detail, dict):
            message = str(exc.detail.get("message", "Request failed"))
            code = str(exc.detail.get("code", f"http_{exc.status_code}"))
            details = exc.detail.get("details")
            run_id = exc.detail.get("run_id") or getattr(request.state, "run_id", None)
        else:
            message = str(exc.detail)
            code = f"http_{exc.status_code}"
            details = None
            run_id = getattr(request.state, "run_id", None)

        return _build_error_response(
            status_code=exc.status_code,
            code=code,
            message=message,
            details=details,
            run_id=run_id,
        )

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception):
        return _build_error_response(
            status_code=500,
            code="internal_server_error",
            message="Pipeline execution failed",
            run_id=getattr(request.state, "run_id", None),
        )
