from processing.feature_engineering import add_resolution_metrics, add_time_features


def test_feature_engineering_builds_resolution_and_sla_flags(spark):
    df = spark.createDataFrame(
        [
            {
                "unique_key": "1",
                "created_date": "2026-01-01T00:00:00",
                "due_date": "2026-01-03T00:00:00",
                "closed_date": "2026-01-04T00:00:00",
            },
            {
                "unique_key": "2",
                "created_date": "bad-date",
                "due_date": "2026-01-03T00:00:00",
                "closed_date": "bad-date",
            },
        ]
    )

    out = add_resolution_metrics(add_time_features(df)).orderBy("unique_key").collect()

    assert out[0]["resolution_completed"] == 1
    assert out[0]["sla_breach"] == 1
    assert out[1]["created_ts_parse_failed"] == 1
    assert out[1]["closed_ts_parse_failed"] == 1
    assert out[1]["sla_breach"] is None
