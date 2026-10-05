from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from pyspark.sql import DataFrame


@dataclass(frozen=True)
class AgentResult:
    status: str
    data: dict[str, Any]
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ValidationAgentInput:
    df: DataFrame


@dataclass(frozen=True)
class ValidationAgentOutput(AgentResult):
    pass


@dataclass(frozen=True)
class AnalyticsAgentInput:
    df: DataFrame


@dataclass(frozen=True)
class AnalyticsAgentOutput(AgentResult):
    pass


@dataclass(frozen=True)
class InsightAgentInput:
    analytics_output: AnalyticsAgentOutput | dict[str, Any]


@dataclass(frozen=True)
class InsightAgentOutput(AgentResult):
    pass


@dataclass(frozen=True)
class RecommendationAgentInput:
    insights_output: InsightAgentOutput | dict[str, Any]


@dataclass(frozen=True)
class RecommendationAgentOutput(AgentResult):
    pass
