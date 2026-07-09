from agents.base_agent import BaseAgent

class InsightAgent(BaseAgent):

    def __init__(self):
        super().__init__("InsightAgent")

    def run(self, analytics_output):

        insights = []

        if analytics_output["sla_rate"] < 0.7:
            insights.append("Low SLA compliance detected across agencies.")

        if analytics_output["backlog_ratio"] > 0.3:
            insights.append("High backlog observed. Service delays likely increasing.")

        if analytics_output["avg_resolution_time"] > 5:
            insights.append("Resolution times are significantly high.")

        return {"insights": insights}