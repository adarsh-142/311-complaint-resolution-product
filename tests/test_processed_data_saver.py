import json
from pathlib import Path

from core.config import settings
from processing.processed_data_saver import save_processed_data
from processing.spark_session import get_spark_session


def test_save_processed_data_writes_a_json_file(tmp_path):
    spark = get_spark_session("test-save")
    original_processed_path = settings.PROCESSED_PATH
    try:
        settings.PROCESSED_PATH = str(tmp_path)
        df = spark.createDataFrame([("A", "2026-06-01")], ["unique_key", "created_date"])

        output_path = save_processed_data(df)

        output_file = Path(output_path)
        assert output_file.exists(), f"Expected output file at {output_path}"
        assert output_file.is_file(), (
            f"Expected a file, got a directory or other path: {output_path}"
        )

        content = output_file.read_text(encoding="utf-8")
        rows = json.loads(content)
        assert len(rows) == 1
        assert rows[0]["unique_key"] == "A"
    finally:
        settings.PROCESSED_PATH = original_processed_path
        spark.stop()
