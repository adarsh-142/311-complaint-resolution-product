from agents.recommendation_agent import RecommendationAgent


def test_duplicate_triggers_collapse_into_one_semantic_category():
    agent = RecommendationAgent()

    result = agent.run(
        {
            "insights": [
                "Overall SLA compliance is 82.0% (breach rate 18.0%), below a 90% target.",
                "Overall SLA compliance is 82.0% (breach rate 18.0%), below a 90% target.",
                "Backlog is elevated at 35.0% of requests, indicating unresolved workload accumulation.",
                "Backlog is elevated at 35.0% of requests, indicating unresolved workload accumulation.",
            ],
            "signals": {},
        }
    )

    assert result.status == "success"
    assert result.errors == []
    recommendations = result.data["recommendations"]

    assert [item["category"] for item in recommendations] == [
        "sla_management",
        "backlog_management",
    ]
    assert len(recommendations) == 2

    sla_recommendation = recommendations[0]
    backlog_recommendation = recommendations[1]

    assert sla_recommendation["id"] == "REC-002"
    assert len(sla_recommendation["evidence"]) == 2
    assert sla_recommendation["strongest_evidence"]["evidence_strength"] == 85
    assert backlog_recommendation["id"] == "REC-003"
    assert len(backlog_recommendation["evidence"]) == 2


def test_conflicting_conditions_preserve_strongest_evidence_with_stable_ordering():
    agent = RecommendationAgent()

    result = agent.run(
        {
            "insights": [
                "System efficiency score is in the 'needs_attention' band and requires active operational intervention.",
                "System efficiency score is in the 'critical' band and requires active operational intervention.",
                "Highest overall service-impact complaint categories are: Noise, Heat.",
            ],
            "signals": {
                "priority_complaint_types": ["Noise", "Heat"],
                "priority_boroughs": ["BROOKLYN"],
            },
        }
    )

    assert result.status == "success"
    recommendations = result.data["recommendations"]

    assert [item["category"] for item in recommendations] == [
        "service_recovery",
        "complaint_type_redesign",
        "borough_operations",
    ]

    service_recovery = recommendations[0]
    complaint_redesign = recommendations[1]
    borough_ops = recommendations[2]

    assert service_recovery["id"] == "REC-001"
    assert len(service_recovery["evidence"]) == 2
    assert service_recovery["strongest_evidence"]["trigger"].lower().find("critical") != -1
    assert service_recovery["strongest_evidence"]["evidence_strength"] == 100

    assert complaint_redesign["id"] == "REC-005"
    assert complaint_redesign["strongest_evidence"]["source"] == "signal"
    assert complaint_redesign["strongest_evidence"]["signal_key"] == "priority_complaint_types"

    assert borough_ops["id"] == "REC-006"
    assert borough_ops["strongest_evidence"]["source"] == "signal"
    assert borough_ops["strongest_evidence"]["signal_key"] == "priority_boroughs"


def test_fallback_recommendations_remain_stable_when_no_signals_exist():
    agent = RecommendationAgent()

    result = agent.run({"insights": [], "signals": {}})

    assert result.status == "success"
    recommendations = result.data["recommendations"]

    assert [item["id"] for item in recommendations] == ["REC-900", "REC-901"]
    assert recommendations[0]["category"] == "monitoring"
    assert recommendations[1]["category"] == "alerting"
