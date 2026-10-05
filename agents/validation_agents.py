import logging
from typing import Any

from pyspark.sql.functions import col, when
from pyspark.sql.functions import sum as spark_sum

from agents.base_agent import BaseAgent
from agents.contracts import ValidationAgentInput, ValidationAgentOutput
from processing.spark_operations import safe_collect, safe_count

logger = logging.getLogger(__name__)


class ValidationAgent(BaseAgent):
    def __init__(self):
        super().__init__("ValidationAgent")

    def run(self, agent_input: ValidationAgentInput | Any):
        df = agent_input.df if isinstance(agent_input, ValidationAgentInput) else agent_input

        logger.info("Starting data validation...")
        total_rows = safe_count(df, timeout_seconds=600, description="total rows")

        null_counts = {}
        if df.columns:
            null_agg = df.agg(
                *[
                    spark_sum(when(col(column_name).isNull(), 1).otherwise(0)).alias(column_name)
                    for column_name in df.columns
                ]
            )
            null_row = safe_collect(null_agg, timeout_seconds=600, description="null counts")[0]
            null_counts = {
                column_name: int(null_row[column_name] or 0) for column_name in df.columns
            }

        non_null_coverage = {}
        if total_rows > 0:
            non_null_coverage = {
                column_name: float((total_rows - null_count) / total_rows)
                for column_name, null_count in null_counts.items()
            }

        logger.info(f"✅ Validation complete - {total_rows} total rows")

        return ValidationAgentOutput(
            status="success",
            data={
                "total_rows": int(total_rows),
                "null_counts": null_counts,
                "non_null_coverage": non_null_coverage,
            },
            warnings=[],
            errors=[],
        )
