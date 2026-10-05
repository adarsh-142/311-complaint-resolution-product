from ingestion.schema_validation import normalize_schema, validate_schema


def test_normalize_schema_aliases(spark):
    df = spark.createDataFrame(
        [
            {
                "uniqueKey": "1",
                "created_ts": "2026-01-01T00:00:00",
                "due_ts": "2026-01-03T00:00:00",
                "closed_ts": "2026-01-02T00:00:00",
                "agency_name": "DSNY",
                "complaint_category": "Noise",
                "borough_name": "BROOKLYN",
            }
        ]
    )

    normalized_df, warnings = normalize_schema(df)

    assert "unique_key" in normalized_df.columns
    assert "created_date" in normalized_df.columns
    assert warnings


def test_validate_schema_flags_missing_required_fields(spark):
    df = spark.createDataFrame(
        [
            {
                "unique_key": "1",
                "created_date": "2026-01-01T00:00:00",
                "due_date": "bad-date",
                "closed_date": "2026-01-02T00:00:00",
                "agency": "DSNY",
                "borough": "BROOKLYN",
            }
        ]
    )

    result = validate_schema(df)

    assert result.is_valid is False
    assert "complaint_type" in result.quality_metrics.missing_fields


def test_validate_schema_flags_malformed_dates_when_schema_present(spark):
    df = spark.createDataFrame(
        [
            {
                "unique_key": "1",
                "created_date": "2026-01-01T00:00:00",
                "due_date": "bad-date",
                "closed_date": "2026-01-02T00:00:00",
                "agency": "DSNY",
                "complaint_type": "Noise",
                "borough": "BROOKLYN",
            }
        ]
    )

    result = validate_schema(df)

    assert result.is_valid is False
    assert result.quality_metrics.malformed_date_counts["due_date"] == 1
