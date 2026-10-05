import os
import sys

from core.config import settings


def _append_env_path(name: str, value: str) -> None:
    existing = os.environ.get(name, "")
    if not existing:
        os.environ[name] = value
        return
    values = existing.split(os.pathsep)
    if value not in values:
        os.environ[name] = existing + os.pathsep + value


def _get_windows_short_path(path: str) -> str:
    """Return 8.3 short path on Windows to avoid issues with spaces in paths.

    Falls back to original path if the conversion fails or is not available.
    """
    if os.name != "nt":
        return path

    try:
        import ctypes

        GetShortPathNameW = ctypes.windll.kernel32.GetShortPathNameW
        GetShortPathNameW.argtypes = [ctypes.c_wchar_p, ctypes.c_wchar_p, ctypes.c_uint]
        GetShortPathNameW.restype = ctypes.c_uint

        # first call to get required buffer size
        buf_size = GetShortPathNameW(path, None, 0)
        if buf_size == 0:
            return path

        buf = ctypes.create_unicode_buffer(buf_size)
        res = GetShortPathNameW(path, buf, buf_size)
        if res == 0:
            return path

        return buf.value
    except Exception:
        return path


def get_spark_session(app_name: str = "AgenticAnalyticsSystem"):
    py_exec = sys.executable
    py_exec_short = _get_windows_short_path(py_exec)

    os.environ["PYSPARK_PYTHON"] = py_exec_short
    os.environ["PYSPARK_DRIVER_PYTHON"] = py_exec_short
    os.environ["PYSPARK_DRIVER_PYTHON_OPTS"] = ""
    os.environ.setdefault("SPARK_LOCAL_IP", settings.SPARK_LOCAL_IP)
    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", settings.SPARK_LOCAL_HOSTNAME)
    _append_env_path("PYTHONPATH", os.getcwd())

    if settings.HADOOP_HOME:
        os.environ.setdefault("HADOOP_HOME", settings.HADOOP_HOME)
    if settings.HADOOP_BIN_DIR:
        _append_env_path("PATH", settings.HADOOP_BIN_DIR)

    if settings.PYSPARK_SUBMIT_ARGS:
        os.environ.setdefault("PYSPARK_SUBMIT_ARGS", settings.PYSPARK_SUBMIT_ARGS)

    os.makedirs(settings.SPARK_LOCAL_DIR, exist_ok=True)
    os.makedirs(settings.SPARK_WAREHOUSE_DIR, exist_ok=True)

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName(app_name)
        .master(settings.SPARK_MASTER)
        .config("spark.driver.bindAddress", settings.SPARK_DRIVER_BIND_ADDRESS)
        .config("spark.driver.host", settings.SPARK_DRIVER_HOST)
        .config("spark.sql.shuffle.partitions", str(settings.SPARK_SHUFFLE_PARTITIONS))
        .config("spark.default.parallelism", str(settings.SPARK_DEFAULT_PARALLELISM))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.local.dir", settings.SPARK_LOCAL_DIR)
        .config("spark.sql.warehouse.dir", settings.SPARK_WAREHOUSE_DIR)
        .config("spark.python.worker.reuse", "false")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .config("spark.sql.execution.wholeStageCodegen.enabled", "false")
        .config("spark.python.worker.faulthandler.enabled", "true")
        .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true")
        .config("spark.pyspark.python", py_exec_short)
        .config("spark.pyspark.driver.python", py_exec_short)
        .config("spark.executorEnv.PYSPARK_PYTHON", py_exec_short)
        .config("spark.executorEnv.PYSPARK_DRIVER_PYTHON", py_exec_short)
        .config("spark.network.timeout", "600s")
        .config("spark.executor.heartbeatInterval", "90s")
        .config("spark.sql.broadcastTimeout", "600")
        .config("spark.shuffle.io.retryWaits", "10s,60s")
        .config("spark.scheduler.listenerBus.eventQueue.capacity", "10000")
        .config("spark.sql.streaming.checkpointLocation.delete.on.stop", "true")
        .config("spark.python.worker.socket.connectTimeout", "600")
        .config("spark.python.worker.socket.timeout", "600")
        .getOrCreate()
    )

    print("SPARK SESSION CREATED")
    return spark
