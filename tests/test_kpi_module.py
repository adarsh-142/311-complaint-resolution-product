from processing.feature_engineering import add_resolution_metrics, add_time_features
from processing.kpi_module import compute_operational_kpis
from processing.spark_session import get_spark_session


def _build_fixture_df(spark):
    raw_df = spark.createDataFrame(
        [
            (
                "1",
                "2026-01-01T00:00:00",
                "2026-01-03T00:00:00",
                "2026-01-02T00:00:00",
                "DSNY",
                "Noise",
                "BROOKLYN",
                "Closed",
            ),
            (
                "2",
                "2026-01-01T00:00:00",
                "2026-01-02T00:00:00",
                "2026-01-04T00:00:00",
                "DSNY",
                "Noise",
                "BROOKLYN",
                "Closed",
            ),
            (
                "3",
                "2026-01-02T00:00:00",
                "2026-01-03T00:00:00",
                None,
                "DOB",
                "Heat",
                "QUEENS",
                "Open",
            ),
            (
                "4",
                "2026-01-02T00:00:00",
                "2026-01-04T00:00:00",
                "2026-01-03T00:00:00",
                "DOB",
                "Heat",
                "QUEENS",
                "Closed",
            ),
            (
                "5",
                "2026-01-03T00:00:00",
                "2026-01-05T00:00:00",
                "2026-01-07T00:00:00",
                "NYPD",
                "Illegal Parking",
                "BRONX",
                "Closed",
            ),
            (
                "6",
                "2026-01-03T00:00:00",
                "2026-01-04T00:00:00",
                None,
                "NYPD",
                "Illegal Parking",
                "BRONX",
                "Open",
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
            "status",
        ],
    )

    return add_resolution_metrics(add_time_features(raw_df))


def test_compute_operational_kpis_returns_expected_summary_and_bounds():
    spark = get_spark_session("test-kpi-module")
    try:
        df = _build_fixture_df(spark)
        kpis = compute_operational_kpis(df, top_n=2, trend_points=2, agency_category_limit=3)

        assert kpis["total_requests"] == 6
        assert kpis["open_requests"] == 2
        assert kpis["closed_requests"] == 4
        assert kpis["open_rate"] == 2 / 6
        assert kpis["closed_rate"] == 4 / 6

        assert kpis["avg_resolution_time"] == 2.25
        assert kpis["median_resolution_time"] == 1.0
        assert kpis["sla_breach_count"] == 2
        assert kpis["sla_breach_rate"] == 0.5
        assert kpis["sla_compliance_rate"] == 0.5

        assert len(kpis["time_trends"]) <= 2
        assert len(kpis["top_complaint_types"]) <= 2
        assert len(kpis["top_agencies"]) <= 2
        assert len(kpis["borough_performance"]) <= 2
        assert len(kpis["agency_category_performance"]) <= 3

        assert kpis["top_complaint_types"][0]["complaint_type"] == "Heat"
        assert kpis["top_agencies"][0]["agency"] == "DOB"
        assert kpis["borough_performance"][0]["borough"] == "BRONX"

        buckets = [entry["age_bucket"] for entry in kpis["backlog_ageing"]]
        assert buckets
        assert "90d+" in buckets or "31-90d" in buckets or "8-30d" in buckets or "0-7d" in buckets
    finally:
        spark.stop()
