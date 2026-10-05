import os
import tempfile
from datetime import datetime, timedelta


def _as_bool(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


class Settings:
    APP_NAME = "311 Complaints Analytics System"
    APP_VERSION = os.getenv("APP_VERSION", "1.1.0")
    API_DOCS_URL = os.getenv("API_DOCS_URL", "/docs")
    API_OPENAPI_URL = os.getenv("API_OPENAPI_URL", "/openapi.json")
    DATA_PATH = os.getenv("DATA_PATH", "data/raw/")
    PROCESSED_PATH = os.getenv("PROCESSED_PATH", "data/processed/")
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    API_URL = os.getenv("API_URL", "https://data.cityofnewyork.us/resource/erm2-nwe9.json")
    API_LIMIT = int(os.getenv("API_LIMIT", 25000))
    API_BATCH_SIZE = int(os.getenv("API_BATCH_SIZE", 1000))
    API_TIMEOUT = int(os.getenv("API_TIMEOUT", 240))
    MAX_UPLOAD_BYTES = int(os.getenv("MAX_UPLOAD_BYTES", str(10 * 1024 * 1024)))
    ALLOWED_UPLOAD_CONTENT_TYPES = tuple(
        item.strip().lower()
        for item in os.getenv(
            "ALLOWED_UPLOAD_CONTENT_TYPES",
            "application/json,text/json,application/x-ndjson,application/octet-stream",
        ).split(",")
        if item.strip()
    )
    ALLOWED_UPLOAD_EXTENSIONS = tuple(
        item.strip().lower()
        for item in os.getenv("ALLOWED_UPLOAD_EXTENSIONS", ".json,.ndjson").split(",")
        if item.strip()
    )

    TIMESTAMP_FORMATS = tuple(
        fmt.strip()
        for fmt in os.getenv(
            "TIMESTAMP_FORMATS",
            "yyyy-MM-dd'T'HH:mm:ss|yyyy-MM-dd HH:mm:ss|yyyy-MM-dd",
        ).split("|")
        if fmt.strip()
    )

    SPARK_EXECUTION_MODE = os.getenv("SPARK_EXECUTION_MODE", "local").strip().lower()
    SPARK_MASTER = os.getenv(
        "SPARK_MASTER",
        "local[*]" if SPARK_EXECUTION_MODE == "local" else "local[*]",
    )
    SPARK_LOCAL_IP = os.getenv("SPARK_LOCAL_IP", "127.0.0.1")
    SPARK_LOCAL_HOSTNAME = os.getenv("SPARK_LOCAL_HOSTNAME", "localhost")
    SPARK_DRIVER_BIND_ADDRESS = os.getenv("SPARK_DRIVER_BIND_ADDRESS", "127.0.0.1")
    SPARK_DRIVER_HOST = os.getenv("SPARK_DRIVER_HOST", "127.0.0.1")
    SPARK_LOCAL_DIR = os.getenv(
        "SPARK_LOCAL_DIR",
        os.path.join(tempfile.gettempdir(), "spark-local"),
    )
    SPARK_WAREHOUSE_DIR = os.getenv(
        "SPARK_WAREHOUSE_DIR",
        os.path.join(tempfile.gettempdir(), "spark-warehouse"),
    )
    PYSPARK_SUBMIT_ARGS = os.getenv("PYSPARK_SUBMIT_ARGS")
    HADOOP_HOME = os.getenv("HADOOP_HOME")
    HADOOP_BIN_DIR = os.getenv("HADOOP_BIN_DIR")
    SPARK_SHUFFLE_PARTITIONS = int(
        os.getenv("SPARK_SHUFFLE_PARTITIONS", "2" if SPARK_EXECUTION_MODE == "local" else "200")
    )
    SPARK_DEFAULT_PARALLELISM = int(
        os.getenv("SPARK_DEFAULT_PARALLELISM", "2" if SPARK_EXECUTION_MODE == "local" else "200")
    )

    SAVE_PROCESSED_SNAPSHOT = _as_bool(os.getenv("SAVE_PROCESSED_SNAPSHOT"), True)
    PROCESSED_WRITE_PARTITIONS = int(
        os.getenv("PROCESSED_WRITE_PARTITIONS", "1" if SPARK_EXECUTION_MODE == "local" else "8")
    )

    # Date filtering (default: last 30 days)
    _end_date = datetime.now()
    _start_date = _end_date - timedelta(days=30)
    API_START_DATE = os.getenv("API_START_DATE", _start_date.strftime("%Y-%m-%d"))
    API_END_DATE = os.getenv("API_END_DATE", _end_date.strftime("%Y-%m-%d"))


settings = Settings()
