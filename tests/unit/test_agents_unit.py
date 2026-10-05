from agents.analytics_agent import AnalyticsAgent
from agents.insights_agent import InsightAgent
from agents.recommendation_agent import RecommendationAgent
from agents.validation_agents import ValidationAgent
from processing.feature_engineering import add_resolution_metrics, add_time_features


def test_validation_agent_returns_coverage(df_311_small):
    out = ValidationAgent().run(df_311_small)

    assert out.status == "success"
    assert out.data["total_rows"] == 4
    assert "non_null_coverage" in out.data


def test_analytics_agent_returns_kpi_payload(df_311_small):
    engineered = add_resolution_metrics(add_time_features(df_311_small))

    out = AnalyticsAgent().run(engineered)

    assert out.status == "success"
    assert out.data["total_requests"] == 4
    assert "top_complaint_types" in out.data


def test_insight_agent_derives_signals_from_analytics_payload():
    analytics_output = {
        "status": "success",
        "data": {
            "sla_compliance_rate": 0.5,
            "sla_breach_rate": 0.5,
            "backlog_ratio": 0.4,
            "avg_resolution_time": 5.0,
            "p90_resolution_time": 9.0,
            "ses_band": "needs_attention",
            "total_requests": 1000,
            "complaint_type_impact_top": [
                {"complaint_type": "Heat"},
                {"complaint_type": "Noise"},
            ],
            "borough_stats": [
                {"borough": "QUEENS", "total_requests": 600, "avg_resolution_time": 7.0}
            ],
        },
    }

    out = InsightAgent().run(analytics_output)

    assert out.status == "success"
    assert out.data["insights"]
    assert "Heat" in out.data["signals"]["priority_complaint_types"]


def test_recommendation_agent_is_deterministic_and_bounded():
    insights_output = {
        "status": "success",
        "data": {
            "insights": [
                "Overall SLA compliance is 72.0% (breach rate 28.0%), below a 90% target.",
                "Backlog is elevated at 35.0% of requests, indicating unresolved workload accumulation.",
            ],
            "signals": {
                "priority_complaint_types": ["Heat", "Noise"],
                "priority_boroughs": ["QUEENS"],
            },
        },
    }

    out = RecommendationAgent().run(insights_output)

    assert out.status == "success"
    recommendations = out.data["recommendations"]
    assert recommendations
    assert recommendations == sorted(recommendations, key=lambda row: (row["priority"], row["id"]))
    assert len({row["id"] for row in recommendations}) == len(recommendations)
