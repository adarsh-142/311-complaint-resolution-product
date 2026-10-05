from processing.kpi_module import compute_operational_kpis


def test_kpi_outputs_are_bounded_and_deterministic(df_311_engineered):
    payload = compute_operational_kpis(
        df_311_engineered, top_n=2, trend_points=2, agency_category_limit=2
    )

    assert payload["total_requests"] == 4
    assert payload["open_requests"] == 2
    assert payload["closed_requests"] == 2
    assert len(payload["top_complaint_types"]) <= 2
    assert len(payload["top_agencies"]) <= 2
    assert len(payload["time_trends"]) <= 2
    assert len(payload["agency_category_performance"]) <= 2
    assert "sla_breach_rate" in payload
    assert "ses_band" in payload
