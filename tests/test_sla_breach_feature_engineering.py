from agents.analytics_agent import AnalyticsAgent
from processing.feature_engineering import add_resolution_metrics, add_time_features
from processing.spark_session import get_spark_session


def _build_df(spark, rows):
    schema = [
        "unique_key",
        "created_date",
        "due_date",
        "closed_date",
    ]
    return spark.createDataFrame(rows, schema)


def test_sla_breach_is_one_when_closed_after_due():
    spark = get_spark_session("test-sla-breach-one")
    try:
        df = _build_df(
            spark,
            [
                ("1", "2026-01-01T00:00:00", "2026-01-05T00:00:00", "2026-01-06T00:00:00"),
            ],
        )

        result = add_resolution_metrics(add_time_features(df)).collect()[0]

        assert result["sla_breach"] == 1
        assert result["resolution_completed"] == 1
    finally:
        spark.stop()


def test_sla_breach_is_zero_when_closed_on_or_before_due():
    spark = get_spark_session("test-sla-breach-zero")
    try:
        df = _build_df(
            spark,
            [
                ("1", "2026-01-01T00:00:00", "2026-01-05T00:00:00", "2026-01-05T00:00:00"),
                ("2", "2026-01-01T00:00:00", "2026-01-05T00:00:00", "2026-01-04T00:00:00"),
            ],
        )

        rows = add_resolution_metrics(add_time_features(df)).collect()

        assert [row["sla_breach"] for row in rows] == [0, 0]
    finally:
        spark.stop()


def test_sla_breach_is_null_when_timestamps_are_missing():
    spark = get_spark_session("test-sla-breach-null")
    try:
        df = _build_df(
            spark,
            [
                ("1", None, "2026-01-05T00:00:00", "2026-01-06T00:00:00"),
                ("2", "2026-01-01T00:00:00", None, "2026-01-06T00:00:00"),
                ("3", "2026-01-01T00:00:00", "2026-01-05T00:00:00", None),
            ],
        )

        rows = add_resolution_metrics(add_time_features(df)).collect()

        assert [row["sla_breach"] for row in rows] == [None, None, None]
    finally:
        spark.stop()


def test_sla_breach_is_null_when_timestamp_order_is_invalid():
    spark = get_spark_session("test-sla-breach-invalid-order")
    try:
        df = _build_df(
            spark,
            [
                ("1", "2026-01-05T00:00:00", "2026-01-04T00:00:00", "2026-01-06T00:00:00"),
                ("2", "2026-01-01T00:00:00", "2026-01-05T00:00:00", "2025-12-31T00:00:00"),
            ],
        )

        rows = add_resolution_metrics(add_time_features(df)).collect()

        assert [row["sla_breach"] for row in rows] == [None, None]
    finally:
        spark.stop()


def test_analytics_agent_uses_generated_sla_breach_column():
    spark = get_spark_session("test-analytics-uses-sla-breach")
    try:
        df = spark.createDataFrame(
            [
                (
                    "1",
                    "2026-01-01T00:00:00",
                    "2026-01-05T00:00:00",
                    "2026-01-06T00:00:00",
                    1,
                    2,
                    "BROOKLYN",
                    "Noise",
                ),
                (
                    "2",
                    "2026-01-01T00:00:00",
                    "2026-01-05T00:00:00",
                    "2026-01-04T00:00:00",
                    0,
                    2,
                    "BROOKLYN",
                    "Noise",
                ),
            ],
            [
                "unique_key",
                "created_date",
                "due_date",
                "closed_date",
                "sla_breach",
                "resolution_time_days",
                "borough",
                "complaint_type",
            ],
        )

        result = AnalyticsAgent().run(df)

        assert result.status == "success"
        assert result.data["total_requests"] == 2
        assert result.data["sla_breach_rate"] == 0.5
        assert result.data["sla_compliance_rate"] == 0.5
    finally:
        spark.stop()


def test_time_features_tracks_timestamp_parse_failures():
    spark = get_spark_session("test-time-feature-parse-failures")
    try:
        df = _build_df(
            spark,
            [
                ("1", "2026-01-01T00:00:00", "bad-date", "2026-01-02T00:00:00"),
                ("2", "bad-date", "2026-01-03T00:00:00", "bad-date"),
            ],
        )

        rows = add_time_features(df).orderBy("unique_key").collect()

        assert rows[0]["created_ts_parse_failed"] == 0
        assert rows[0]["due_ts_parse_failed"] == 1
        assert rows[0]["closed_ts_parse_failed"] == 0

        assert rows[1]["created_ts_parse_failed"] == 1
        assert rows[1]["due_ts_parse_failed"] == 0
        assert rows[1]["closed_ts_parse_failed"] == 1
    finally:
        spark.stop()
