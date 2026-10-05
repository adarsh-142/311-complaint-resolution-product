from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from agents.base_agent import BaseAgent
from agents.contracts import RecommendationAgentInput, RecommendationAgentOutput


@dataclass(frozen=True)
class RecommendationRule:
    id: str
    category: str
    priority: int
    recommendation: str


@dataclass(frozen=True)
class RecommendationEvidence:
    source: str
    trigger: str
    evidence_strength: int
    insight_index: int | None = None
    signal_key: str | None = None
    signal_value: Any = None


RULES = [
    RecommendationRule(
        id="REC-001",
        category="service_recovery",
        priority=10,
        recommendation="Launch a 30-day service recovery plan with daily KPI reviews covering compliance rate, backlog burn-down, and p90 resolution time.",
    ),
    RecommendationRule(
        id="REC-002",
        category="sla_management",
        priority=20,
        recommendation="Review SLA ownership by agency and enforce response-time playbooks for high-volume request categories.",
    ),
    RecommendationRule(
        id="REC-003",
        category="backlog_management",
        priority=30,
        recommendation="Create a rolling backlog-burn plan with weekly targets and dedicated teams for unresolved tickets.",
    ),
    RecommendationRule(
        id="REC-004",
        category="resolution_flow",
        priority=40,
        recommendation="Segment requests by complexity and route complex cases to specialized queues to reduce long-tail resolution delays.",
    ),
    RecommendationRule(
        id="REC-005",
        category="complaint_type_redesign",
        priority=50,
        recommendation="Prioritize process redesign and staffing for these complaint types.",
    ),
    RecommendationRule(
        id="REC-006",
        category="borough_operations",
        priority=60,
        recommendation="Deploy targeted field support and operational monitoring in the affected boroughs.",
    ),
]


def _build_evidence(
    source: str, trigger: str, evidence_strength: int, **extra: Any
) -> dict[str, Any]:
    payload = {
        "source": source,
        "trigger": trigger,
        "evidence_strength": int(evidence_strength),
    }
    payload.update({key: value for key, value in extra.items() if value is not None})
    return payload


class RecommendationAgent(BaseAgent):
    def __init__(self):
        super().__init__("RecommendationAgent")

    def _rule_by_category(self, category: str) -> RecommendationRule:
        for rule in RULES:
            if rule.category == category:
                return rule
        raise KeyError(category)

    def _match_insight_evidence(
        self, insight: str, insight_index: int
    ) -> list[tuple[str, RecommendationEvidence]]:
        normalized = insight.lower()
        matches: list[tuple[str, RecommendationEvidence]] = []

        if "critical" in normalized or "needs_attention" in normalized:
            matches.append(
                (
                    "service_recovery",
                    RecommendationEvidence(
                        source="insight",
                        trigger=insight,
                        evidence_strength=100 if "critical" in normalized else 90,
                        insight_index=insight_index,
                    ),
                )
            )

        if "sla compliance" in normalized or "breach rate" in normalized:
            matches.append(
                (
                    "sla_management",
                    RecommendationEvidence(
                        source="insight",
                        trigger=insight,
                        evidence_strength=85 if "breach rate" in normalized else 75,
                        insight_index=insight_index,
                    ),
                )
            )

        if "backlog" in normalized:
            matches.append(
                (
                    "backlog_management",
                    RecommendationEvidence(
                        source="insight",
                        trigger=insight,
                        evidence_strength=80,
                        insight_index=insight_index,
                    ),
                )
            )

        if "resolution time" in normalized or "long-tail" in normalized:
            matches.append(
                (
                    "resolution_flow",
                    RecommendationEvidence(
                        source="insight",
                        trigger=insight,
                        evidence_strength=70,
                        insight_index=insight_index,
                    ),
                )
            )

        if (
            "service-impact complaint categories" in normalized
            or "impact complaint categories" in normalized
        ):
            matches.append(
                (
                    "complaint_type_redesign",
                    RecommendationEvidence(
                        source="insight",
                        trigger=insight,
                        evidence_strength=65,
                        insight_index=insight_index,
                    ),
                )
            )

        if "boroughs" in normalized and "slower" in normalized:
            matches.append(
                (
                    "borough_operations",
                    RecommendationEvidence(
                        source="insight",
                        trigger=insight,
                        evidence_strength=65,
                        insight_index=insight_index,
                    ),
                )
            )

        return matches

    def _match_signal_evidence(
        self, signals: dict[str, Any]
    ) -> list[tuple[str, RecommendationEvidence]]:
        matches: list[tuple[str, RecommendationEvidence]] = []

        priority_types = signals.get("priority_complaint_types", []) or []
        if priority_types:
            matches.append(
                (
                    "complaint_type_redesign",
                    RecommendationEvidence(
                        source="signal",
                        trigger="priority_complaint_types",
                        evidence_strength=95,
                        signal_key="priority_complaint_types",
                        signal_value=list(priority_types),
                    ),
                )
            )

        priority_boroughs = signals.get("priority_boroughs", []) or []
        if priority_boroughs:
            matches.append(
                (
                    "borough_operations",
                    RecommendationEvidence(
                        source="signal",
                        trigger="priority_boroughs",
                        evidence_strength=95,
                        signal_key="priority_boroughs",
                        signal_value=list(priority_boroughs),
                    ),
                )
            )

        return matches

    def _aggregate_recommendations(
        self, matches: list[tuple[str, RecommendationEvidence]]
    ) -> list[dict[str, Any]]:
        grouped: dict[str, dict[str, Any]] = {}

        for category, evidence in matches:
            rule = self._rule_by_category(category)
            bucket = grouped.setdefault(
                category,
                {
                    "id": rule.id,
                    "category": rule.category,
                    "priority": rule.priority,
                    "recommendation": rule.recommendation,
                    "evidence": [],
                    "strongest_evidence": None,
                },
            )

            evidence_payload = _build_evidence(
                source=evidence.source,
                trigger=evidence.trigger,
                evidence_strength=evidence.evidence_strength,
                insight_index=evidence.insight_index,
                signal_key=evidence.signal_key,
                signal_value=evidence.signal_value,
            )
            bucket["evidence"].append(evidence_payload)

            if (
                bucket["strongest_evidence"] is None
                or evidence_payload["evidence_strength"]
                > bucket["strongest_evidence"]["evidence_strength"]
                or (
                    evidence_payload["evidence_strength"]
                    == bucket["strongest_evidence"]["evidence_strength"]
                    and evidence_payload["trigger"] < bucket["strongest_evidence"]["trigger"]
                )
            ):
                bucket["strongest_evidence"] = evidence_payload

        ordered = sorted(grouped.values(), key=lambda item: (item["priority"], item["id"]))

        for item in ordered:
            item["evidence"] = sorted(
                item["evidence"],
                key=lambda evidence: (-evidence["evidence_strength"], evidence["trigger"]),
            )

        return ordered

    def run(self, agent_input: RecommendationAgentInput | dict):
        insights_output = (
            agent_input.insights_output
            if isinstance(agent_input, RecommendationAgentInput)
            else agent_input
        )
        insights_data = insights_output.get("data", insights_output)
        insights = insights_data.get("insights", []) or []
        signals = insights_data.get("signals", {}) or {}

        matches: list[tuple[str, RecommendationEvidence]] = []
        for insight_index, insight in enumerate(insights):
            matches.extend(self._match_insight_evidence(insight, insight_index))

        matches.extend(self._match_signal_evidence(signals))

        recommendations = self._aggregate_recommendations(matches)

        if not recommendations:
            fallback_trigger = "no actionable insights were generated"
            recommendations = [
                {
                    "id": "REC-900",
                    "category": "monitoring",
                    "priority": 900,
                    "recommendation": "Maintain current operating cadence and continue monitoring SLA, backlog, and resolution-time trends.",
                    "evidence": [
                        _build_evidence(
                            source="fallback",
                            trigger=fallback_trigger,
                            evidence_strength=1,
                        )
                    ],
                    "strongest_evidence": _build_evidence(
                        source="fallback",
                        trigger=fallback_trigger,
                        evidence_strength=1,
                    ),
                },
                {
                    "id": "REC-901",
                    "category": "alerting",
                    "priority": 910,
                    "recommendation": "Set automated alert thresholds for sudden changes in complaint volume, breach rate, or unresolved workload.",
                    "evidence": [
                        _build_evidence(
                            source="fallback",
                            trigger=fallback_trigger,
                            evidence_strength=1,
                        )
                    ],
                    "strongest_evidence": _build_evidence(
                        source="fallback",
                        trigger=fallback_trigger,
                        evidence_strength=1,
                    ),
                },
            ]

        return RecommendationAgentOutput(
            status="success",
            data={"recommendations": recommendations},
            warnings=[],
            errors=[],
        )
