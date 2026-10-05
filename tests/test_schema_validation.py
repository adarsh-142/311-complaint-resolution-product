from pyspark.sql.types import StringType, StructField, StructType

from ingestion.schema_validation import normalize_schema, validate_schema
from processing.spark_session import get_spark_session


def _base_rows():
    return [
        (
            "1",
            "2026-01-01T00:00:00",
            "2026-01-03T00:00:00",
            "2026-01-02T00:00:00",
            "DSNY",
            "Noise",
            "BROOKLYN",
        ),
        (
            "2",
            "2026-01-02T00:00:00",
            "2026-01-04T00:00:00",
            "2026-01-04T00:00:00",
            "DSNY",
            "Noise",
            "BROOKLYN",
        ),
    ]


def _build_df(spark, rows, schema=None):
    schema = schema or [
        "unique_key",
        "created_date",
        "due_date",
        "closed_date",
        "agency",
        "complaint_type",
        "borough",
    ]
    return spark.createDataFrame(rows, schema)


def test_valid_input_returns_typed_result():
    spark = get_spark_session("test-validation-valid")
    try:
        df = _build_df(spark, _base_rows())
        normalized_df, alias_warnings = normalize_schema(df)
        result = validate_schema(normalized_df, warnings=alias_warnings)

        assert result.is_valid is True
        assert result.errors == []
        assert result.row_count == 2
        assert result.quality_metrics.missing_fields == []
    finally:
        spark.stop()


def test_missing_column_is_fatal():
    spark = get_spark_session("test-validation-missing-column")
    try:
        df = spark.createDataFrame(
            [
                ("1", "2026-01-01T00:00:00", "2026-01-03T00:00:00", "DSNY", "Noise", "BROOKLYN"),
            ],
            ["unique_key", "created_date", "due_date", "agency", "complaint_type", "borough"],
        )

        result = validate_schema(df)

        assert result.is_valid is False
        assert any("Missing required fields" in error for error in result.errors)
        assert "closed_date" in result.quality_metrics.missing_fields
    finally:
        spark.stop()


def test_empty_input_is_fatal():
    spark = get_spark_session("test-validation-empty")
    try:
        schema = StructType(
            [
                StructField("unique_key", StringType(), True),
                StructField("created_date", StringType(), True),
                StructField("due_date", StringType(), True),
                StructField("closed_date", StringType(), True),
                StructField("agency", StringType(), True),
                StructField("complaint_type", StringType(), True),
                StructField("borough", StringType(), True),
            ]
        )
        df = spark.createDataFrame([], schema)

        result = validate_schema(df)

        assert result.is_valid is False
        assert any("empty" in error.lower() for error in result.errors)
        assert result.row_count == 0
    finally:
        spark.stop()


def test_malformed_date_is_fatal():
    spark = get_spark_session("test-validation-malformed-date")
    try:
        df = spark.createDataFrame(
            [
                (
                    "1",
                    "2026-01-01T00:00:00",
                    "not-a-date",
                    "2026-01-02T00:00:00",
                    "DSNY",
                    "Noise",
                    "BROOKLYN",
                ),
            ],
            [
                "unique_key",
                "created_date",
                "due_date",
                "closed_date",
                "agency",
                "complaint_type",
                "borough",
            ],
        )

        result = validate_schema(df)

        assert result.is_valid is False
        assert any("malformed timestamp" in error.lower() for error in result.errors)
        assert result.quality_metrics.malformed_date_counts["due_date"] == 1
    finally:
        spark.stop()


def test_high_null_input_produces_warning_but_stays_valid():
    spark = get_spark_session("test-validation-high-null")
    try:
        df = spark.createDataFrame(
            [
                (
                    "1",
                    "2026-01-01T00:00:00",
                    "2026-01-03T00:00:00",
                    "2026-01-02T00:00:00",
                    None,
                    "Noise",
                    "BROOKLYN",
                ),
                (
                    "2",
                    "2026-01-02T00:00:00",
                    "2026-01-04T00:00:00",
                    "2026-01-04T00:00:00",
                    None,
                    "Noise",
                    "BROOKLYN",
                ),
                (
                    "3",
                    "2026-01-03T00:00:00",
                    "2026-01-05T00:00:00",
                    "2026-01-05T00:00:00",
                    None,
                    "Noise",
                    "BROOKLYN",
                ),
                (
                    "4",
                    "2026-01-04T00:00:00",
                    "2026-01-06T00:00:00",
                    "2026-01-06T00:00:00",
                    "DSNY",
                    "Noise",
                    "BROOKLYN",
                ),
            ],
            [
                "unique_key",
                "created_date",
                "due_date",
                "closed_date",
                "agency",
                "complaint_type",
                "borough",
            ],
        )

        result = validate_schema(df)

        assert result.is_valid is True
        assert result.errors == []
        assert result.warnings
        assert "agency" in result.quality_metrics.high_null_fields
    finally:
        spark.stop()


def test_accepted_aliases_are_normalized():
    spark = get_spark_session("test-validation-aliases")
    try:
        df = spark.createDataFrame(
            [
                (
                    "1",
                    "2026-01-01T00:00:00",
                    "2026-01-03T00:00:00",
                    "2026-01-02T00:00:00",
                    "DSNY",
                    "Noise",
                    "BROOKLYN",
                ),
            ],
            [
                "uniqueKey",
                "created_ts",
                "due_ts",
                "closed_ts",
                "agency_name",
                "complaint_category",
                "borough_name",
            ],
        )

        normalized_df, alias_warnings = normalize_schema(df)
        result = validate_schema(normalized_df, warnings=alias_warnings)

        assert result.is_valid is True
        assert normalized_df.columns == [
            "unique_key",
            "created_date",
            "due_date",
            "closed_date",
            "agency",
            "complaint_type",
            "borough",
        ]
    finally:
        spark.stop()
