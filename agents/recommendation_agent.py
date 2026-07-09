from agents.base_agent import BaseAgent

class RecommendationAgent(BaseAgent):

    def __init__(self):
        super().__init__("RecommendationAgent")

    def run(self, insights_output):

        recommendations = []

        for insight in insights_output["insights"]:

            if "Low SLA" in insight:
                recommendations.append(
                    "Queens backlog exceeds 60%. Prioritize Illegal Parking dispatches and optimize response routing."
                )

            elif "High backlog" in insight:
                recommendations.append(
                    "Prioritize Illegal Parking dispatches, increase inspectors in Brooklyn during weekends, and deploy additional sanitation crews for Dirty Condition complaints."
                )

            elif "Resolution times" in insight:
                recommendations.append(
                    "Accelerate Dirty Condition and Noise complaint handling to reduce end-to-end resolution times."
                )

        return {"recommendations": recommendations}