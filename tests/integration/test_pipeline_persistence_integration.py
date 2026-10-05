from ingestion.schema_validation import ValidationQualityMetrics, ValidationResult
from services import pipeline_services


class StubOrchestrator:
    def run(self, df, validation_result=None):
        return {
            "run_id": "offline-run",
            "status": "success",
            "stage_order": ["validation", "analytics", "insight", "recommendation"],
            "started_at": "2026-01-01T00:00:00Z",
            "completed_at": "2026-01-01T00:00:01Z",
            "duration_seconds": 1.0,
            "errors": [],
            "stages": {
                "validation": {"status": "success", "duration_seconds": 0.1},
                "analytics": {"status": "success", "duration_seconds": 0.1},
                "insight": {"status": "success", "duration_seconds": 0.1},
                "recommendation": {"status": "success", "duration_seconds": 0.1},
            },
            "validation": {"status": "success", "data": {}, "warnings": [], "errors": []},
            "analytics": {"status": "success", "data": {}, "warnings": [], "errors": []},
            "insights": {"status": "success", "data": {}, "warnings": [], "errors": []},
            "recommendations": {"status": "success", "data": {}, "warnings": [], "errors": []},
            "timings": {
                "validation": 0.1,
                "analytics": 0.1,
                "insights": 0.1,
                "recommendations": 0.1,
            },
        }


def _validation_success():
    return ValidationResult(
        is_valid=True,
        errors=[],
        warnings=[],
        row_count=1,
        quality_metrics=ValidationQualityMetrics(
            required_fields=[],
            available_fields=[],
            missing_fields=[],
            alias_resolution={},
            null_counts={},
            null_rates={},
            malformed_date_counts={},
            high_null_fields=[],
        ),
    )


def test_pipeline_saves_processed_snapshot_when_enabled(monkeypatch, spark):
    save_calls = {"count": 0}

    input_records = [
        {
            "unique_key": "1",
            "created_date": "2026-01-01T00:00:00",
            "due_date": "2026-01-02T00:00:00",
            "closed_date": "2026-01-02T00:00:00",
            "agency": "DSNY",
            "complaint_type": "Noise",
            "borough": "BROOKLYN",
            "status": "Closed",
        }
    ]
    input_df = spark.createDataFrame(input_records)

    monkeypatch.setattr(pipeline_services, "fetch_311_data", lambda **kwargs: input_records)
    monkeypatch.setattr(pipeline_services, "save_raw_data", lambda payload: "ignored.json")
    monkeypatch.setattr(pipeline_services, "get_spark_session", lambda: spark)
    monkeypatch.setattr(pipeline_services, "load_data", lambda spark_session, file_path: input_df)
    monkeypatch.setattr(pipeline_services, "clean_data", lambda df: df)
    monkeypatch.setattr(pipeline_services, "WorkflowOrchestrator", StubOrchestrator)
    monkeypatch.setattr(
        pipeline_services, "validate_schema", lambda df, warnings=None: _validation_success()
    )

    def _save_processed(df):
        save_calls["count"] += 1
        return "processed.json"

    monkeypatch.setattr(pipeline_services, "save_processed_data", _save_processed)
    monkeypatch.setattr(pipeline_services.settings, "SAVE_PROCESSED_SNAPSHOT", True)

    result = pipeline_services.run_pipeline("2026-01-01", "2026-01-31")

    assert result["status"] == "success"
    assert save_calls["count"] == 1


def test_pipeline_skips_processed_snapshot_when_disabled(monkeypatch, spark):
    save_calls = {"count": 0}

    input_records = [
        {
            "unique_key": "1",
            "created_date": "2026-01-01T00:00:00",
            "due_date": "2026-01-02T00:00:00",
            "closed_date": "2026-01-02T00:00:00",
            "agency": "DSNY",
            "complaint_type": "Noise",
            "borough": "BROOKLYN",
            "status": "Closed",
        }
    ]
    input_df = spark.createDataFrame(input_records)

    monkeypatch.setattr(pipeline_services, "fetch_311_data", lambda **kwargs: input_records)
    monkeypatch.setattr(pipeline_services, "save_raw_data", lambda payload: "ignored.json")
    monkeypatch.setattr(pipeline_services, "get_spark_session", lambda: spark)
    monkeypatch.setattr(pipeline_services, "load_data", lambda spark_session, file_path: input_df)
    monkeypatch.setattr(pipeline_services, "clean_data", lambda df: df)
    monkeypatch.setattr(pipeline_services, "WorkflowOrchestrator", StubOrchestrator)
    monkeypatch.setattr(
        pipeline_services, "validate_schema", lambda df, warnings=None: _validation_success()
    )

    def _save_processed(df):
        save_calls["count"] += 1
        return "processed.json"

    monkeypatch.setattr(pipeline_services, "save_processed_data", _save_processed)
    monkeypatch.setattr(pipeline_services.settings, "SAVE_PROCESSED_SNAPSHOT", False)

    result = pipeline_services.run_pipeline("2026-01-01", "2026-01-31")

    assert result["status"] == "success"
    assert save_calls["count"] == 0
    assert result["timings"]["save_processed"] == 0.0
