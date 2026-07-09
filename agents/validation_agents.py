from agents.base_agent import BaseAgent
from processing.spark_operations import safe_count
import logging

logger = logging.getLogger(__name__)

class ValidationAgent(BaseAgent):

    def __init__(self):
        super().__init__("ValidationAgent")

    def run(self, df):

        logger.info("Starting data validation...")
        total_rows = safe_count(df, timeout_seconds=600, description="total rows")
        
        null_counts = {}
        for col in ["agency", "complaint_type", "borough"]:
            if col in df.columns:
                null_count = safe_count(
                    df.filter(df[col].isNull()), 
                    timeout_seconds=600, 
                    description=f"null count for {col}"
                )
                null_counts[col] = int(null_count)
        
        logger.info(f"✅ Validation complete - {total_rows} total rows")
        
        return {
            "total_rows": int(total_rows),
            "null_counts": null_counts
        }