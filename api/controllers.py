from __future__ import annotations

import os
import tempfile
from pathlib import Path

from fastapi import HTTPException, Request, Response, UploadFile

from api.schemas import HealthResponse, RunAnalysisRequest
from core.config import settings
from services.pipeline_services import run_pipeline, run_pipeline_from_file
from utils.json_safety import to_json_safe


def _extract_run_id(payload: dict) -> str | None:
    run_id = payload.get("run_id")
    if run_id:
        return str(run_id)

    nested = payload.get("results")
    if isinstance(nested, dict) and nested.get("run_id"):
        return str(nested.get("run_id"))
    return None


def _finalize_response(response: Response, request: Request, safe_result: dict) -> str | None:
    run_id = _extract_run_id(safe_result)
    if run_id:
        request.state.run_id = run_id
        response.headers["X-Run-ID"] = run_id
    return run_id


def health_status() -> HealthResponse:
    return HealthResponse(
        app_name=settings.APP_NAME,
        version=settings.APP_VERSION,
        docs_url=settings.API_DOCS_URL,
        openapi_url=settings.API_OPENAPI_URL,
        timestamp_utc=HealthResponse.now_iso(),
    )


def run_analysis(request: Request, response: Response, payload: RunAnalysisRequest):
    try:
        result = run_pipeline(
            start_date=payload.start_date.isoformat() if payload.start_date else None,
            end_date=payload.end_date.isoformat() if payload.end_date else None,
        )
    except Exception:
        raise HTTPException(
            status_code=500,
            detail={
                "code": "internal_server_error",
                "message": "Pipeline execution failed",
                "run_id": getattr(request.state, "run_id", None),
            },
        )
    safe_result = to_json_safe(result)
    run_id = _finalize_response(response=response, request=request, safe_result=safe_result)

    if safe_result.get("status") == "validation_error":
        raise HTTPException(
            status_code=422,
            detail={
                "code": "validation_error",
                "message": safe_result.get("message", "Input validation failed"),
                "details": safe_result.get("details", safe_result),
                "run_id": run_id,
            },
        )

    if safe_result.get("status") == "error":
        raise HTTPException(
            status_code=500,
            detail={
                "code": "pipeline_error",
                "message": safe_result.get("message", "Pipeline execution failed"),
                "details": {"timings": safe_result.get("timings", {})},
                "run_id": run_id,
            },
        )

    return {"status": "success", "run_id": run_id, "data": safe_result}


async def run_analysis_file(request: Request, response: Response, file: UploadFile):
    if not file.filename:
        raise HTTPException(
            status_code=400,
            detail={"code": "missing_filename", "message": "Uploaded file must include a filename"},
        )

    suffix = Path(file.filename).suffix.lower()
    if suffix not in settings.ALLOWED_UPLOAD_EXTENSIONS:
        raise HTTPException(
            status_code=415,
            detail={
                "code": "unsupported_file_extension",
                "message": f"Unsupported file extension '{suffix}'. Allowed: {settings.ALLOWED_UPLOAD_EXTENSIONS}",
            },
        )

    content_type = (file.content_type or "application/octet-stream").lower()
    if content_type not in settings.ALLOWED_UPLOAD_CONTENT_TYPES:
        raise HTTPException(
            status_code=415,
            detail={
                "code": "unsupported_media_type",
                "message": f"Unsupported content type '{content_type}'. Allowed: {settings.ALLOWED_UPLOAD_CONTENT_TYPES}",
            },
        )

    total_bytes = 0
    temp_path = None
    try:
        with tempfile.NamedTemporaryFile(mode="wb", suffix=suffix, delete=False) as temp_file:
            temp_path = temp_file.name
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                total_bytes += len(chunk)
                if total_bytes > settings.MAX_UPLOAD_BYTES:
                    raise HTTPException(
                        status_code=413,
                        detail={
                            "code": "file_too_large",
                            "message": f"Uploaded file exceeds {settings.MAX_UPLOAD_BYTES} bytes",
                            "details": {"max_upload_bytes": settings.MAX_UPLOAD_BYTES},
                        },
                    )
                temp_file.write(chunk)

        try:
            result = run_pipeline_from_file(temp_path)
        except Exception:
            raise HTTPException(
                status_code=500,
                detail={
                    "code": "internal_server_error",
                    "message": "Pipeline execution failed",
                    "run_id": getattr(request.state, "run_id", None),
                },
            )
        safe_result = to_json_safe(result)
        run_id = _finalize_response(response=response, request=request, safe_result=safe_result)

        if safe_result.get("status") == "validation_error":
            raise HTTPException(
                status_code=422,
                detail={
                    "code": "validation_error",
                    "message": safe_result.get("message", "Input validation failed"),
                    "details": safe_result.get("details", safe_result),
                    "run_id": run_id,
                },
            )

        if safe_result.get("status") == "error":
            raise HTTPException(
                status_code=500,
                detail={
                    "code": "pipeline_error",
                    "message": safe_result.get("message", "Pipeline execution failed"),
                    "details": {"timings": safe_result.get("timings", {})},
                    "run_id": run_id,
                },
            )

        return {"status": "success", "run_id": run_id, "data": safe_result}
    finally:
        try:
            await file.close()
        finally:
            if temp_path and os.path.exists(temp_path):
                os.remove(temp_path)
