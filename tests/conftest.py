import os
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from processing.feature_engineering import add_resolution_metrics, add_time_features


@pytest.fixture()
def spark(tmp_path_factory):
    base_dir = tmp_path_factory.mktemp("spark-local")
    warehouse_dir = Path(base_dir, "warehouse")
    local_dir = Path(base_dir, "local")
    warehouse_dir.mkdir(parents=True, exist_ok=True)
    local_dir.mkdir(parents=True, exist_ok=True)

    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    session = (
        SparkSession.builder.appName("tests-offline")
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.warehouse.dir", str(warehouse_dir))
        .config("spark.local.dir", str(local_dir))
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config("spark.executorEnv.PYSPARK_PYTHON", sys.executable)
        .config("spark.executorEnv.PYSPARK_DRIVER_PYTHON", sys.executable)
        .getOrCreate()
    )

    yield session
    session.stop()


@pytest.fixture()
def records_311_small():
    return [
        {
            "unique_key": "1",
            "created_date": "2026-01-01T00:00:00",
            "due_date": "2026-01-02T00:00:00",
            "closed_date": "2026-01-02T00:00:00",
            "agency": "DSNY",
            "complaint_type": "Noise",
            "borough": "BROOKLYN",
            "status": "Closed",
        },
        {
            "unique_key": "2",
            "created_date": "2026-01-01T08:00:00",
            "due_date": "2026-01-03T00:00:00",
            "closed_date": "2026-01-04T00:00:00",
            "agency": "DOB",
            "complaint_type": "Heat",
            "borough": "QUEENS",
            "status": "Closed",
        },
        {
            "unique_key": "3",
            "created_date": "2026-01-02 09:30:00",
            "due_date": "2026-01-04 00:00:00",
            "closed_date": None,
            "agency": "DOB",
            "complaint_type": "Heat",
            "borough": "QUEENS",
            "status": "Open",
        },
        {
            "unique_key": "4",
            "created_date": "2026-01-03",
            "due_date": "2026-01-05",
            "closed_date": "bad-date",
            "agency": "NYPD",
            "complaint_type": "Illegal Parking",
            "borough": "BRONX",
            "status": "Open",
        },
    ]


@pytest.fixture()
def df_311_small(spark, records_311_small):
    return spark.createDataFrame(records_311_small)


@pytest.fixture()
def df_311_engineered(df_311_small):
    return add_resolution_metrics(add_time_features(df_311_small))
