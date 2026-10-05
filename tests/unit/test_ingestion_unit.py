import json

import pytest

from ingestion.api_client import _build_params, _normalize_date_window
from ingestion.data_loader import load_data


def test_load_data_reads_json_array(spark, tmp_path, records_311_small):
    data_file = tmp_path / "records.json"
    data_file.write_text(json.dumps(records_311_small), encoding="utf-8")

    df = load_data(spark, str(data_file), file_type="json")

    assert df.count() == len(records_311_small)
    assert set(["unique_key", "created_date", "due_date"]).issubset(set(df.columns))


def test_load_data_rejects_unsupported_file_type(spark, tmp_path):
    dummy = tmp_path / "dummy.txt"
    dummy.write_text("x", encoding="utf-8")

    with pytest.raises(ValueError, match="Unsupported file type"):
        load_data(spark, str(dummy), file_type="csv")


def test_normalize_date_window_swaps_and_caps_future_dates():
    start, end = _normalize_date_window("2036-01-10", "2036-01-01")

    assert start <= end
    assert len(start) == 10
    assert len(end) == 10


def test_build_params_uses_date_filter_clause():
    params = _build_params(
        batch_size=100,
        offset=200,
        start_date="2026-01-01",
        end_date="2026-01-31",
        use_date_filter=True,
    )

    assert params["$limit"] == 100
    assert params["$offset"] == 200
    assert "$where" in params
    assert "2026-01-01T00:00:00" in params["$where"]
