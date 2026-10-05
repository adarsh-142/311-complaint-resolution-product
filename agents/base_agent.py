class BaseAgent:
    def __init__(self, name: str):
        self.name = name

    def run(self, agent_input):
        raise NotImplementedError("Each agent must implement run()")
