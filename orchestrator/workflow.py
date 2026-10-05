import time
import uuid
from datetime import datetime, timezone
from typing import Any

from agents.analytics_agent import AnalyticsAgent
from agents.contracts import (
    AnalyticsAgentInput,
    InsightAgentInput,
    RecommendationAgentInput,
    ValidationAgentInput,
)
from agents.insights_agent import InsightAgent
from agents.recommendation_agent import RecommendationAgent
from agents.validation_agents import ValidationAgent


class WorkflowOrchestrator:
    STAGE_ORDER = ["validation", "analytics", "insight", "recommendation"]

    def __init__(self):
        self.validation_agent = ValidationAgent()
        self.analytics_agent = AnalyticsAgent()
        self.insight_agent = InsightAgent()
        self.recommendation_agent = RecommendationAgent()

    def _utc_now(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _stage_record(self, stage_name: str) -> dict[str, Any]:
        return {
            "name": stage_name,
            "status": "pending",
            "started_at": None,
            "completed_at": None,
            "duration_seconds": 0.0,
            "fatal": False,
            "warnings": [],
            "errors": [],
            "data": {},
        }

    def _start_stage(self, stage: dict[str, Any]) -> float:
        stage["status"] = "running"
        stage["started_at"] = self._utc_now()
        return time.time()

    def _finish_stage_success(
        self, stage: dict[str, Any], start_clock: float, payload: dict[str, Any]
    ):
        stage["status"] = "success"
        stage["completed_at"] = self._utc_now()
        stage["duration_seconds"] = time.time() - start_clock
        stage["warnings"] = list(payload.get("warnings", []))
        stage["errors"] = list(payload.get("errors", []))
        stage["data"] = dict(payload.get("data", {}))

    def _finish_stage_failure(
        self, stage: dict[str, Any], start_clock: float, error: str, fatal: bool = True
    ):
        stage["status"] = "failed"
        stage["completed_at"] = self._utc_now()
        stage["duration_seconds"] = time.time() - start_clock
        stage["fatal"] = fatal
        stage["errors"] = [error]

    def _skip_stage(self, stage: dict[str, Any], reason: str):
        stage["status"] = "skipped"
        stage["started_at"] = self._utc_now()
        stage["completed_at"] = stage["started_at"]
        stage["duration_seconds"] = 0.0
        stage["warnings"] = [reason]

    def _normalize_payload(self, payload: Any) -> dict[str, Any]:
        if hasattr(payload, "to_dict"):
            return payload.to_dict()
        if isinstance(payload, dict):
            return payload
        return {
            "status": "error",
            "data": {},
            "warnings": [],
            "errors": [f"Unsupported payload type: {type(payload).__name__}"],
        }

    def run(self, df, validation_result=None):
        run_id = str(uuid.uuid4())
        workflow_started_at = self._utc_now()
        workflow_start_clock = time.time()

        stages = {
            "validation": self._stage_record("validation"),
            "analytics": self._stage_record("analytics"),
            "insight": self._stage_record("insight"),
            "recommendation": self._stage_record("recommendation"),
        }

        validation_output: dict[str, Any] = {}
        analytics_output: dict[str, Any] = {}
        insights_output: dict[str, Any] = {}
        recommendations_output: dict[str, Any] = {}
        workflow_errors: list[str] = []

        # Stage 1: Validation
        validation_clock = self._start_stage(stages["validation"])
        try:
            if validation_result is not None:
                normalized_validation = self._normalize_payload(validation_result)

                if hasattr(validation_result, "is_valid"):
                    normalized_validation = {
                        "status": "success" if validation_result.is_valid else "error",
                        "data": normalized_validation,
                        "warnings": normalized_validation.get("warnings", []),
                        "errors": normalized_validation.get("errors", []),
                    }

                validation_output = normalized_validation
            else:
                validation_output = self._normalize_payload(
                    self.validation_agent.run(ValidationAgentInput(df=df))
                )

            validation_status = validation_output.get("status", "error")
            if validation_status != "success":
                validation_errors = validation_output.get("errors") or ["Validation failed"]
                self._finish_stage_failure(
                    stages["validation"],
                    validation_clock,
                    "; ".join(str(error) for error in validation_errors),
                    fatal=True,
                )
                workflow_errors.extend([str(error) for error in validation_errors])
            else:
                self._finish_stage_success(
                    stages["validation"], validation_clock, validation_output
                )
        except Exception as exc:
            self._finish_stage_failure(stages["validation"], validation_clock, str(exc), fatal=True)
            workflow_errors.append(f"validation: {exc}")

        if stages["validation"]["status"] == "failed":
            self._skip_stage(stages["analytics"], "Skipped because validation failed.")
            self._skip_stage(stages["insight"], "Skipped because validation failed.")
            self._skip_stage(stages["recommendation"], "Skipped because validation failed.")
        else:
            # Stage 2: Analytics
            analytics_clock = self._start_stage(stages["analytics"])
            try:
                analytics_output = self._normalize_payload(
                    self.analytics_agent.run(AnalyticsAgentInput(df=df))
                )
                if analytics_output.get("status") != "success":
                    analytics_errors = analytics_output.get("errors") or ["Analytics failed"]
                    self._finish_stage_failure(
                        stages["analytics"],
                        analytics_clock,
                        "; ".join(str(error) for error in analytics_errors),
                        fatal=True,
                    )
                    workflow_errors.extend([str(error) for error in analytics_errors])
                else:
                    self._finish_stage_success(
                        stages["analytics"], analytics_clock, analytics_output
                    )
            except Exception as exc:
                self._finish_stage_failure(
                    stages["analytics"], analytics_clock, str(exc), fatal=True
                )
                workflow_errors.append(f"analytics: {exc}")

            if stages["analytics"]["status"] == "failed":
                self._skip_stage(stages["insight"], "Skipped because analytics failed.")
                self._skip_stage(stages["recommendation"], "Skipped because analytics failed.")
            else:
                # Stage 3: Insight
                insight_clock = self._start_stage(stages["insight"])
                try:
                    insights_output = self._normalize_payload(
                        self.insight_agent.run(InsightAgentInput(analytics_output=analytics_output))
                    )
                    if insights_output.get("status") != "success":
                        insight_errors = insights_output.get("errors") or [
                            "Insight generation failed"
                        ]
                        self._finish_stage_failure(
                            stages["insight"],
                            insight_clock,
                            "; ".join(str(error) for error in insight_errors),
                            fatal=True,
                        )
                        workflow_errors.extend([str(error) for error in insight_errors])
                    else:
                        self._finish_stage_success(
                            stages["insight"], insight_clock, insights_output
                        )
                except Exception as exc:
                    self._finish_stage_failure(
                        stages["insight"], insight_clock, str(exc), fatal=True
                    )
                    workflow_errors.append(f"insight: {exc}")

                if stages["insight"]["status"] == "failed":
                    self._skip_stage(stages["recommendation"], "Skipped because insight failed.")
                else:
                    # Stage 4: Recommendation
                    recommendation_clock = self._start_stage(stages["recommendation"])
                    try:
                        recommendations_output = self._normalize_payload(
                            self.recommendation_agent.run(
                                RecommendationAgentInput(insights_output=insights_output)
                            )
                        )
                        if recommendations_output.get("status") != "success":
                            recommendation_errors = recommendations_output.get("errors") or [
                                "Recommendation generation failed"
                            ]
                            self._finish_stage_failure(
                                stages["recommendation"],
                                recommendation_clock,
                                "; ".join(str(error) for error in recommendation_errors),
                                fatal=True,
                            )
                            workflow_errors.extend([str(error) for error in recommendation_errors])
                        else:
                            self._finish_stage_success(
                                stages["recommendation"],
                                recommendation_clock,
                                recommendations_output,
                            )
                    except Exception as exc:
                        self._finish_stage_failure(
                            stages["recommendation"], recommendation_clock, str(exc), fatal=True
                        )
                        workflow_errors.append(f"recommendation: {exc}")

        completed_at = self._utc_now()
        duration_seconds = time.time() - workflow_start_clock
        workflow_status = "failed" if workflow_errors else "success"

        timings = {
            "validation": stages["validation"]["duration_seconds"],
            "analytics": stages["analytics"]["duration_seconds"],
            "insights": stages["insight"]["duration_seconds"],
            "recommendations": stages["recommendation"]["duration_seconds"],
        }

        return {
            "run_id": run_id,
            "status": workflow_status,
            "stage_order": list(self.STAGE_ORDER),
            "started_at": workflow_started_at,
            "completed_at": completed_at,
            "duration_seconds": duration_seconds,
            "errors": workflow_errors,
            "stages": stages,
            "validation": validation_output,
            "analytics": analytics_output,
            "insights": insights_output,
            "recommendations": recommendations_output,
            "timings": timings,
        }
