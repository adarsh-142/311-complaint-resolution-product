from agents.base_agent import BaseAgent
from agents.contracts import InsightAgentInput, InsightAgentOutput


class InsightAgent(BaseAgent):
    def __init__(self):
        super().__init__("InsightAgent")

    def run(self, agent_input: InsightAgentInput | dict):
        analytics_output = (
            agent_input.analytics_output
            if isinstance(agent_input, InsightAgentInput)
            else agent_input
        )
        analytics_data = analytics_output.get("data", analytics_output)
        insights = []
        signals = {"priority_complaint_types": [], "priority_boroughs": []}

        sla_compliance_rate = float(
            analytics_data.get("sla_compliance_rate", analytics_data.get("sla_rate", 0.0))
        )
        sla_breach_rate = float(analytics_data.get("sla_breach_rate", 1.0 - sla_compliance_rate))
        backlog_ratio = float(analytics_data.get("backlog_ratio", 0.0))
        avg_resolution_time = float(analytics_data.get("avg_resolution_time", 0.0))
        p90_resolution_time = float(analytics_data.get("p90_resolution_time", 0.0))
        ses_band = analytics_data.get("ses_band", "unknown")

        if sla_compliance_rate < 0.90:
            insights.append(
                f"Overall SLA compliance is {sla_compliance_rate:.1%} (breach rate {sla_breach_rate:.1%}), below a 90% target."
            )

        if backlog_ratio > 0.20:
            insights.append(
                f"Backlog is elevated at {backlog_ratio:.1%} of requests, indicating unresolved workload accumulation."
            )

        if avg_resolution_time > 3 or p90_resolution_time > 7:
            insights.append(
                "Resolution time distribution is stretched, with long-tail delays affecting user experience."
            )

        impact_types = analytics_data.get("complaint_type_impact_top", [])
        if impact_types:
            top_types = [row.get("complaint_type", "Unknown") for row in impact_types[:5]]
            signals["priority_complaint_types"] = top_types
            insights.append(
                "Highest overall service-impact complaint categories are: "
                + ", ".join(top_types)
                + "."
            )

        borough_stats = analytics_data.get("borough_stats", [])
        if borough_stats and avg_resolution_time > 0:
            total_requests = int(analytics_data.get("total_requests", 0) or 0)
            borough_volume_floor = max(100, int(total_requests * 0.005))
            slow_boroughs = [
                row
                for row in borough_stats
                if row.get("total_requests", 0) >= borough_volume_floor
                and row.get("avg_resolution_time", 0.0) > avg_resolution_time * 1.15
            ]
            slow_boroughs = sorted(
                slow_boroughs, key=lambda x: x.get("avg_resolution_time", 0.0), reverse=True
            )
            if slow_boroughs:
                top_boroughs = [row.get("borough", "Unknown") for row in slow_boroughs[:3]]
                signals["priority_boroughs"] = top_boroughs
                insights.append(
                    "Some boroughs have materially slower resolution performance than the system average: "
                    + ", ".join(top_boroughs)
                    + "."
                )

        if ses_band in {"needs_attention", "critical"}:
            insights.append(
                f"System efficiency score is in the '{ses_band}' band and requires active operational intervention."
            )

        if not insights:
            insights.append(
                "Current performance indicators look stable with no major system-wide anomalies."
            )

        return InsightAgentOutput(
            status="success",
            data={
                "insights": insights,
                "signals": signals,
            },
            warnings=[],
            errors=[],
        )
