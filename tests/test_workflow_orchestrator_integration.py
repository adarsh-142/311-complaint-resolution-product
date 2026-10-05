from dataclasses import dataclass

from orchestrator.workflow import WorkflowOrchestrator
from processing.feature_engineering import add_resolution_metrics, add_time_features
from processing.spark_session import get_spark_session


@dataclass
class StubValidationFailure:
    is_valid: bool = False

    def to_dict(self):
        return {
            "is_valid": False,
            "errors": ["Missing required fields: due_date"],
            "warnings": [],
            "row_count": 0,
            "quality_metrics": {
                "required_fields": ["unique_key", "created_date", "due_date", "closed_date"],
                "available_fields": ["unique_key", "created_date", "closed_date"],
                "missing_fields": ["due_date"],
                "alias_resolution": {},
                "null_counts": {},
                "null_rates": {},
                "malformed_date_counts": {},
                "high_null_fields": [],
            },
        }


def _build_featured_df(spark):
    base_df = spark.createDataFrame(
        [
            (
                "1",
                "2026-01-01T00:00:00",
                "2026-01-05T00:00:00",
                "2026-01-04T00:00:00",
                "BROOKLYN",
                "Noise",
                "Open",
            ),
            (
                "2",
                "2026-01-02T00:00:00",
                "2026-01-04T00:00:00",
                "2026-01-06T00:00:00",
                "QUEENS",
                "Heat",
                "Closed",
            ),
        ],
        [
            "unique_key",
            "created_date",
            "due_date",
            "closed_date",
            "borough",
            "complaint_type",
            "status",
        ],
    )
    return add_resolution_metrics(add_time_features(base_df))


def test_orchestrator_successful_order_and_stage_telemetry():
    spark = get_spark_session("test-orchestrator-success")
    try:
        df = _build_featured_df(spark)
        orchestrator = WorkflowOrchestrator()

        result = orchestrator.run(df)

        assert result["status"] == "success"
        assert result["run_id"]
        assert result["stage_order"] == ["validation", "analytics", "insight", "recommendation"]

        assert result["stages"]["validation"]["status"] == "success"
        assert result["stages"]["analytics"]["status"] == "success"
        assert result["stages"]["insight"]["status"] == "success"
        assert result["stages"]["recommendation"]["status"] == "success"

        assert result["stages"]["validation"]["started_at"]
        assert result["stages"]["validation"]["completed_at"]
        assert result["stages"]["analytics"]["duration_seconds"] >= 0
        assert result["stages"]["insight"]["duration_seconds"] >= 0
        assert result["stages"]["recommendation"]["duration_seconds"] >= 0

        assert result["timings"]["validation"] >= 0
        assert result["timings"]["analytics"] >= 0
        assert result["timings"]["insights"] >= 0
        assert result["timings"]["recommendations"] >= 0

        assert result["recommendations"]["status"] == "success"
        assert isinstance(result["recommendations"]["data"].get("recommendations", []), list)
    finally:
        spark.stop()


def test_orchestrator_stops_downstream_on_fatal_validation_failure():
    spark = get_spark_session("test-orchestrator-validation-failure")
    try:
        df = spark.createDataFrame(
            [("1", "2026-01-01T00:00:00")],
            ["unique_key", "created_date"],
        )
        orchestrator = WorkflowOrchestrator()

        result = orchestrator.run(df, validation_result=StubValidationFailure())

        assert result["status"] == "failed"
        assert result["stages"]["validation"]["status"] == "failed"
        assert result["stages"]["validation"]["fatal"] is True
        assert result["stages"]["analytics"]["status"] == "skipped"
        assert result["stages"]["insight"]["status"] == "skipped"
        assert result["stages"]["recommendation"]["status"] == "skipped"
        assert result["analytics"] == {}
        assert result["insights"] == {}
        assert result["recommendations"] == {}
        assert result["errors"]
    finally:
        spark.stop()
