from orchestrator.workflow import WorkflowOrchestrator
from processing.feature_engineering import add_resolution_metrics, add_time_features


def test_orchestrator_success_path(df_311_small):
    engineered = add_resolution_metrics(add_time_features(df_311_small))

    result = WorkflowOrchestrator().run(engineered)

    assert result["status"] == "success"
    assert result["stage_order"] == ["validation", "analytics", "insight", "recommendation"]
    assert result["stages"]["validation"]["status"] == "success"
    assert result["stages"]["analytics"]["status"] == "success"
    assert result["stages"]["insight"]["status"] == "success"
    assert result["stages"]["recommendation"]["status"] == "success"


def test_orchestrator_fail_fast_on_validation(df_311_small):
    validation_failure = {
        "status": "error",
        "errors": ["Injected validation failure"],
        "warnings": [],
        "data": {},
    }

    result = WorkflowOrchestrator().run(df_311_small, validation_result=validation_failure)

    assert result["status"] == "failed"
    assert result["stages"]["validation"]["status"] == "failed"
    assert result["stages"]["analytics"]["status"] == "skipped"
    assert result["stages"]["insight"]["status"] == "skipped"
    assert result["stages"]["recommendation"]["status"] == "skipped"
