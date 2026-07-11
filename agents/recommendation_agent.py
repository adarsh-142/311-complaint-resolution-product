from agents.base_agent import BaseAgent

class RecommendationAgent(BaseAgent):

    def __init__(self):
        super().__init__("RecommendationAgent")

    def run(self, insights_output):
        recommendations = []
        insights = insights_output.get("insights", [])
        signals = insights_output.get("signals", {})

        for insight in insights:
            normalized = insight.lower()

            if "sla compliance" in normalized or "breach rate" in normalized:
                recommendations.append(
                    "Review SLA ownership by agency and enforce response-time playbooks for high-volume request categories."
                )

            if "backlog" in normalized:
                recommendations.append(
                    "Create a rolling backlog-burn plan with weekly targets and dedicated teams for unresolved tickets."
                )

            if "resolution time" in normalized or "long-tail" in normalized:
                recommendations.append(
                    "Segment requests by complexity and route complex cases to specialized queues to reduce long-tail resolution delays."
                )

            if "impact complaint categories" in normalized or "service-impact complaint categories" in normalized:
                priority_types = signals.get("priority_complaint_types", [])
                if priority_types:
                    recommendations.append(
                        "Prioritize process redesign and staffing for these complaint types: " + ", ".join(priority_types) + "."
                    )

            if "boroughs" in normalized and "slower" in normalized:
                priority_boroughs = signals.get("priority_boroughs", [])
                if priority_boroughs:
                    recommendations.append(
                        "Deploy targeted field support and operational monitoring in: " + ", ".join(priority_boroughs) + "."
                    )

            if "critical" in normalized or "needs_attention" in normalized:
                recommendations.append(
                    "Launch a 30-day service recovery plan with daily KPI reviews covering compliance rate, backlog burn-down, and p90 resolution time."
                )

        if not recommendations:
            recommendations = [
                "Maintain current operating cadence and continue monitoring SLA, backlog, and resolution-time trends.",
                "Set automated alert thresholds for sudden changes in complaint volume, breach rate, or unresolved workload."
            ]

        # Preserve ordering while removing duplicates.
        unique_recommendations = list(dict.fromkeys(recommendations))

        return {"recommendations": unique_recommendations}