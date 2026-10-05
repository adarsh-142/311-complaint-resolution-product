from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class ErrorBody(BaseModel):
    code: str
    message: str
    details: dict[str, Any] | None = None
    run_id: str | None = None


class ErrorResponse(BaseModel):
    status: Literal["error"] = "error"
    error: ErrorBody


class HealthResponse(BaseModel):
    status: Literal["ok"] = "ok"
    app_name: str
    version: str
    docs_url: str
    openapi_url: str
    timestamp_utc: str

    @staticmethod
    def now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()


class RunAnalysisRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    start_date: date | None = None
    end_date: date | None = None

    @model_validator(mode="after")
    def validate_date_window(self):
        if self.start_date and self.end_date and self.start_date > self.end_date:
            raise ValueError("start_date must be less than or equal to end_date")
        return self


class RunAnalysisSuccessResponse(BaseModel):
    status: Literal["success"] = "success"
    run_id: str | None = None
    data: dict[str, Any] = Field(default_factory=dict)
