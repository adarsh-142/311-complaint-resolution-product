import time
from agents.validation_agents import ValidationAgent
from agents.analytics_agent import AnalyticsAgent
from agents.insights_agent import InsightAgent
from agents.recommendation_agent import RecommendationAgent


class WorkflowOrchestrator:

    def __init__(self):
        self.validation_agent = ValidationAgent()
        self.analytics_agent = AnalyticsAgent()
        self.insight_agent = InsightAgent()
        self.recommendation_agent = RecommendationAgent()

    def run(self, df):
        timings = {}

        # Step 1: Validation
        validation_start = time.time()
        validation_output = self.validation_agent.run(df)
        timings["validation"] = time.time() - validation_start

        # Step 2: Analytics
        analytics_start = time.time()
        analytics_output = self.analytics_agent.run(df)
        timings["analytics"] = time.time() - analytics_start

        # Step 3: Insights
        insights_start = time.time()
        insights_output = self.insight_agent.run(analytics_output)
        timings["insights"] = time.time() - insights_start

        # Step 4: Recommendations
        recommendations_start = time.time()
        recommendations_output = self.recommendation_agent.run(insights_output)
        timings["recommendations"] = time.time() - recommendations_start

        return {
            "validation": validation_output,
            "analytics": analytics_output,
            "insights": insights_output,
            "recommendations": recommendations_output,
            "timings": timings
        }