import json
from pathlib import Path

from fastapi.testclient import TestClient

import api.controllers as controllers
from main import app


def _client() -> TestClient:
    return TestClient(app)


def test_health_includes_metadata():
    response = _client().get("/")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert "app_name" in payload
    assert "version" in payload
    assert "timestamp_utc" in payload
    assert "X-Run-ID" in response.headers


def test_run_analysis_success_propagates_run_id_header(monkeypatch):
    def _fake_pipeline(start_date=None, end_date=None):
        return {
            "status": "success",
            "message": "ok",
            "results": {"run_id": "run-123", "analytics": {}},
            "timings": {},
        }

    monkeypatch.setattr(controllers, "run_pipeline", _fake_pipeline)

    response = _client().post("/run-analysis", json={"start_date": "2026-01-01"})

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "success"
    assert body["run_id"] == "run-123"
    assert response.headers["X-Run-ID"] == "run-123"


def test_run_analysis_validation_error_maps_to_422(monkeypatch):
    def _fake_pipeline(start_date=None, end_date=None):
        return {
            "status": "validation_error",
            "message": "Input validation failed",
            "details": {"errors": ["bad schema"]},
            "results": {"run_id": "run-val-1"},
            "timings": {},
        }

    monkeypatch.setattr(controllers, "run_pipeline", _fake_pipeline)

    response = _client().post("/run-analysis", json={"start_date": "2026-01-01"})

    assert response.status_code == 422
    body = response.json()
    assert body["status"] == "error"
    assert body["error"]["code"] == "validation_error"
    assert body["error"]["run_id"] == "run-val-1"
    assert response.headers["X-Run-ID"] == "run-val-1"


def test_run_analysis_unhandled_exception_maps_to_500(monkeypatch):
    def _boom(start_date=None, end_date=None):
        raise RuntimeError("boom")

    monkeypatch.setattr(controllers, "run_pipeline", _boom)

    response = _client().post("/run-analysis", json={"start_date": "2026-01-01"})

    assert response.status_code == 500
    body = response.json()
    assert body["status"] == "error"
    assert body["error"]["code"] == "internal_server_error"


def test_request_validation_error_envelope():
    response = _client().post("/run-analysis", json={"start_date": "bad-date"})

    assert response.status_code == 422
    body = response.json()
    assert body["status"] == "error"
    assert body["error"]["code"] == "request_validation_error"
    assert body["error"]["run_id"]
    assert body["error"]["run_id"] == response.headers["X-Run-ID"]


def test_file_upload_rejects_unsupported_extension():
    files = {"file": ("bad.csv", b"[]", "application/json")}

    response = _client().post("/run-analysis/file", files=files)

    assert response.status_code == 415
    assert response.json()["error"]["code"] == "unsupported_file_extension"


def test_file_upload_rejects_too_large(monkeypatch):
    monkeypatch.setattr(controllers.settings, "MAX_UPLOAD_BYTES", 5)
    files = {"file": ("sample.json", b"[1,2,3,4,5,6]", "application/json")}

    response = _client().post("/run-analysis/file", files=files)

    assert response.status_code == 413
    assert response.json()["error"]["code"] == "file_too_large"


def test_file_upload_uses_temp_file_and_cleans_up(monkeypatch):
    captured = {"path": None}

    def _fake_pipeline_from_file(file_path):
        captured["path"] = file_path
        assert Path(file_path).exists()
        return {
            "status": "success",
            "message": "ok",
            "results": {"run_id": "run-file-1"},
            "timings": {},
        }

    monkeypatch.setattr(controllers, "run_pipeline_from_file", _fake_pipeline_from_file)

    files = {"file": ("sample.json", b'[{"id":1}]', "application/json")}
    response = _client().post("/run-analysis/file", files=files)

    assert response.status_code == 200
    assert response.json()["run_id"] == "run-file-1"
    assert captured["path"] is not None
    assert not Path(captured["path"]).exists()


def test_openapi_contract_includes_typed_models_and_upload_route():
    spec = _client().get("/openapi.json")

    assert spec.status_code == 200
    payload = spec.json()

    run_analysis_post = payload["paths"]["/run-analysis"]["post"]
    assert "requestBody" in run_analysis_post
    assert "application/json" in run_analysis_post["requestBody"]["content"]
    assert "200" in run_analysis_post["responses"]
    assert "422" in run_analysis_post["responses"]
    assert "500" in run_analysis_post["responses"]

    upload_post = payload["paths"]["/run-analysis/file"]["post"]
    assert "multipart/form-data" in upload_post["requestBody"]["content"]
    assert "413" in upload_post["responses"]
    assert "415" in upload_post["responses"]

    schemas = payload["components"]["schemas"]
    assert "RunAnalysisRequest" in schemas
    assert "RunAnalysisSuccessResponse" in schemas
    assert "ErrorResponse" in schemas
    assert "HealthResponse" in schemas


def test_structured_request_logs_include_run_id(caplog):
    caplog.set_level("INFO", logger="api.request")

    response = _client().get("/")

    assert response.status_code == 200
    records = [
        json.loads(record.message) for record in caplog.records if record.name == "api.request"
    ]

    assert any(entry.get("event") == "request_started" for entry in records)
    completed = [entry for entry in records if entry.get("event") == "request_completed"]
    assert completed
    assert completed[-1]["run_id"] == response.headers["X-Run-ID"]
    assert completed[-1]["path"] == "/"
