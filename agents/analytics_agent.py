from agents.base_agent import BaseAgent
from processing.spark_operations import safe_collect

from pyspark.sql.functions import (
    avg,
    col,
    coalesce,
    count,
    datediff,
    expr,
    greatest,
    lit,
    sum,
    when
)
import logging

logger = logging.getLogger(__name__)

class AnalyticsAgent(BaseAgent):

    def __init__(self):
        super().__init__("AnalyticsAgent")

    # -------------------------------------------------
    # SLA ENGINE (PUT ABOVE run() — helper function)
    # -------------------------------------------------
    def compute_dynamic_sla(self, df):
        default_thresholds = {
            "Illegal Parking": 1.0,
            "Noise": 1.0,
            "Rodents": 5.0,
            "Street Condition": 7.0,
        }

        sla_df = (
            df.groupBy("complaint_type")
            .agg(expr("percentile_approx(resolution_time_days, 0.75)").alias("sla_threshold"))
        )

        default_rows = [
            (complaint_type, threshold)
            for complaint_type, threshold in default_thresholds.items()
        ]
        default_df = df.sparkSession.createDataFrame(default_rows, ["complaint_type", "default_sla_threshold"])

        sla_df = (
            sla_df.join(default_df, on="complaint_type", how="left")
            .withColumn(
                "sla_threshold",
                greatest(
                    coalesce(col("sla_threshold"), lit(0.0)),
                    coalesce(col("default_sla_threshold"), lit(0.0)),
                    lit(1.0),
                ),
            )
            .select("complaint_type", "sla_threshold")
        )

        return sla_df

    # -------------------------------------------------
    # MAIN PIPELINE
    # -------------------------------------------------
    def run(self, df):

        # ==============================
        # STEP 0: Ensure required column
        # ==============================
        if "resolution_time_days" not in df.columns:
            df = df.withColumn(
                "resolution_time_days",
                datediff(col("closed_date"), col("created_date"))
            )

        try:
            df = df.cache()
            df.count()
            logger.info("AnalyticsAgent: cached DataFrame for repeated operations")
        except Exception as e:
            logger.warning(f"AnalyticsAgent: caching failed: {e}")

        # ==============================
        # STEP 1: SLA (dynamic)
        # ==============================
        sla_df = self.compute_dynamic_sla(df)
        df = df.join(sla_df, on="complaint_type", how="left")

        df = df.withColumn(
            "sla_breach",
            col("resolution_time_days") > col("sla_threshold")
        )

        # ==============================
        # STEP 2: Core KPIs
        # ==============================
        summary_df = df.agg(
            count("*").alias("total_requests"),
            avg("resolution_time_days").alias("avg_resolution_time"),
            avg(col("sla_breach").cast("int")).alias("sla_rate"),
            sum(when(col("closed_date").isNull(), 1).otherwise(0)).alias("backlog_count")
        )

        summary_row = safe_collect(summary_df, timeout_seconds=600, description="analytics summary")[0]

        total_requests = int(summary_row["total_requests"] or 0)
        avg_resolution_time = float(summary_row["avg_resolution_time"] or 0.0)
        sla_rate = float(summary_row["sla_rate"] or 0.0)
        backlog = int(summary_row["backlog_count"] or 0)
        backlog_ratio = backlog / total_requests if total_requests else 0.0

        ses = (
            (1 - backlog_ratio) * 0.3 +
            sla_rate * 0.4 +
            (1 / (1 + avg_resolution_time)) * 0.3
        )

        # ==============================
        # STEP 3: Borough and SLA breakdown aggregations (coalesced for efficiency)
        # ==============================
        try:
            # Coalesce before aggregation to reduce task count
            df_coalesced = df.coalesce(10)
            
            borough_agg = df_coalesced.groupBy("borough").agg(
                avg("resolution_time_days").alias("avg_resolution_time"),
                count("*").alias("total_requests")
            )

            sla_agg = df_coalesced.groupBy("complaint_type").agg(
                avg("resolution_time_days").alias("avg_resolution_time"),
                avg(col("sla_breach").cast("int")).alias("breach_rate"),
                avg("sla_threshold").alias("sla_threshold")
            )

            borough_rows = safe_collect(borough_agg, timeout_seconds=600, description="borough aggregation")
            sla_rows = safe_collect(sla_agg, timeout_seconds=600, description="complaint-type SLA aggregation")
        except Exception as e:
            logger.error(f"Aggregation failed, attempting fallback with simpler approach: {e}")
            # Fallback: use simpler count/sum for borough and SLA
            borough_rows = []
            sla_rows = []

        borough_stats = [
            {
                "borough": row["borough"] or "Unknown",
                "avg_resolution_time": float(row["avg_resolution_time"] or 0.0),
                "total_requests": int(row["total_requests"] or 0)
            }
            for row in borough_rows
        ]

        sla_breakdown = [
            {
                "complaint_type": row["complaint_type"],
                "avg_resolution_time": float(row["avg_resolution_time"] or 0.0),
                "sla_threshold": float(row["sla_threshold"] or 0.0),
                "breach_rate": float(row["breach_rate"] or 0.0)
            }
            for row in sla_rows
        ]

        # ==============================
        # STEP 5: FINAL OUTPUT
        # ==============================
        return {
            "total_requests": int(total_requests),
            "sla_rate": float(sla_rate),
            "avg_resolution_time": float(avg_resolution_time),
            "backlog_ratio": float(backlog_ratio),
            "ses_score": float(ses),
            "borough_stats": borough_stats,
            "sla_breakdown": sla_breakdown
        }