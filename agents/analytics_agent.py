from agents.base_agent import BaseAgent
from processing.spark_operations import safe_collect

from pyspark.sql.functions import (
    avg,
    col,
    coalesce,
    count,
    datediff,
    desc,
    expr,
    greatest,
    lit,
    sum as spark_sum,
    when
)
import logging

logger = logging.getLogger(__name__)


class AnalyticsAgent(BaseAgent):

    def __init__(self):
        super().__init__("AnalyticsAgent")

    def compute_dynamic_sla(self, df):
        """Compute SLA thresholds per complaint type with global fallback.

        Uses p75 per complaint type and falls back to global p75 when a type has
        insufficient data. Enforces minimum threshold of 1 day.
        """
        global_sla_row = safe_collect(
            df.agg(expr("percentile_approx(resolution_time_days, 0.75)").alias("global_sla")),
            timeout_seconds=600,
            description="global SLA threshold"
        )[0]
        global_sla = float(global_sla_row["global_sla"] or 1.0)

        sla_df = (
            df.groupBy("complaint_type")
            .agg(expr("percentile_approx(resolution_time_days, 0.75)").alias("sla_threshold"))
            .withColumn(
                "sla_threshold",
                greatest(
                    coalesce(col("sla_threshold"), lit(global_sla)),
                    lit(1.0),
                ),
            )
            .select("complaint_type", "sla_threshold")
        )

        return sla_df

    def _collect_or_empty(self, df, description, timeout_seconds=600):
        try:
            return safe_collect(df, timeout_seconds=timeout_seconds, description=description)
        except Exception as e:
            logger.error(f"{description} failed: {e}")
            return []

    def _ses_band(self, ses_score: float) -> str:
        if ses_score >= 0.75:
            return "healthy"
        if ses_score >= 0.60:
            return "watch"
        if ses_score >= 0.45:
            return "needs_attention"
        return "critical"

    def run(self, df):
        if "resolution_time_days" not in df.columns:
            df = df.withColumn(
                "resolution_time_days",
                datediff(col("closed_date"), col("created_date"))
            )

        # Keep only non-negative resolution times where present.
        df = df.withColumn(
            "resolution_time_days",
            when(col("resolution_time_days").isNull(), None).otherwise(
                greatest(col("resolution_time_days"), lit(0))
            )
        )

        try:
            df = df.cache()
            df.count()
            logger.info("AnalyticsAgent: cached DataFrame for repeated operations")
        except Exception as e:
            logger.warning(f"AnalyticsAgent: caching failed: {e}")

        sla_df = self.compute_dynamic_sla(df)
        df = df.join(sla_df, on="complaint_type", how="left")

        df = df.withColumn(
            "sla_breach",
            when(col("resolution_time_days").isNull(), lit(False)).otherwise(
                col("resolution_time_days") > col("sla_threshold")
            )
        )

        summary_df = df.agg(
            count("*").alias("total_requests"),
            avg("resolution_time_days").alias("avg_resolution_time"),
            expr("percentile_approx(resolution_time_days, 0.5)").alias("median_resolution_time"),
            expr("percentile_approx(resolution_time_days, 0.9)").alias("p90_resolution_time"),
            avg(col("sla_breach").cast("int")).alias("sla_breach_rate"),
            spark_sum(when(col("closed_date").isNull(), 1).otherwise(0)).alias("backlog_count")
        )

        summary_row = safe_collect(summary_df, timeout_seconds=600, description="analytics summary")[0]

        total_requests = int(summary_row["total_requests"] or 0)
        avg_resolution_time = float(summary_row["avg_resolution_time"] or 0.0)
        median_resolution_time = float(summary_row["median_resolution_time"] or 0.0)
        p90_resolution_time = float(summary_row["p90_resolution_time"] or 0.0)
        sla_breach_rate = float(summary_row["sla_breach_rate"] or 0.0)
        sla_compliance_rate = 1.0 - sla_breach_rate
        backlog = int(summary_row["backlog_count"] or 0)
        backlog_ratio = backlog / total_requests if total_requests else 0.0

        ses = (
            (1 - backlog_ratio) * 0.3 +
            sla_compliance_rate * 0.4 +
            (1 / (1 + avg_resolution_time)) * 0.3
        )
        ses_band = self._ses_band(float(ses))

        borough_rows = []
        if "borough" in df.columns:
            borough_agg = (
                df.groupBy(coalesce(col("borough"), lit("Unknown")).alias("borough"))
                .agg(
                    avg("resolution_time_days").alias("avg_resolution_time"),
                    count("*").alias("total_requests"),
                    avg(col("sla_breach").cast("int")).alias("breach_rate")
                )
                .orderBy(desc("total_requests"))
            )
            borough_rows = self._collect_or_empty(borough_agg, "borough aggregation")

        complaint_rows = []
        if "complaint_type" in df.columns:
            complaint_agg = (
                df.groupBy(coalesce(col("complaint_type"), lit("Unknown")).alias("complaint_type"))
                .agg(
                    count("*").alias("total_requests"),
                    avg("resolution_time_days").alias("avg_resolution_time"),
                    avg(col("sla_breach").cast("int")).alias("breach_rate"),
                    avg("sla_threshold").alias("sla_threshold")
                )
                .orderBy(desc("total_requests"))
            )
            complaint_rows = self._collect_or_empty(complaint_agg, "complaint-type SLA aggregation")

        status_rows = []
        if "status" in df.columns:
            status_agg = (
                df.groupBy(coalesce(col("status"), lit("Unknown")).alias("status"))
                .agg(count("*").alias("total_requests"))
                .orderBy(desc("total_requests"))
            )
            status_rows = self._collect_or_empty(status_agg, "status aggregation")

        borough_stats = [
            {
                "borough": row["borough"],
                "avg_resolution_time": float(row["avg_resolution_time"] or 0.0),
                "sla_compliance_rate": float(1.0 - (row["breach_rate"] or 0.0)),
                "sla_breach_rate": float(row["breach_rate"] or 0.0),
                "total_requests": int(row["total_requests"] or 0)
            }
            for row in borough_rows
        ]

        sla_breakdown = [
            {
                "complaint_type": row["complaint_type"],
                "avg_resolution_time": float(row["avg_resolution_time"] or 0.0),
                "sla_threshold": float(row["sla_threshold"] or 0.0),
                "breach_rate": float(row["breach_rate"] or 0.0),
                "compliance_rate": float(1.0 - (row["breach_rate"] or 0.0)),
                "total_requests": int(row["total_requests"] or 0)
            }
            for row in complaint_rows
        ]

        for row in sla_breakdown:
            volume_share = (row["total_requests"] / total_requests) if total_requests else 0.0
            row["volume_share"] = float(volume_share)
            row["impact_score"] = float(volume_share * row["breach_rate"] * max(row["avg_resolution_time"], 0.0))

        # Keep payload compact by default while still exposing high-impact slices.
        top_by_volume = sorted(sla_breakdown, key=lambda x: x["total_requests"], reverse=True)[:20]
        min_impact_volume = max(25, int(total_requests * 0.002))
        impact_candidates = [row for row in sla_breakdown if row["total_requests"] >= min_impact_volume]
        if not impact_candidates:
            impact_candidates = sla_breakdown
        top_by_impact = sorted(impact_candidates, key=lambda x: x["impact_score"], reverse=True)[:10]

        status_breakdown = [
            {
                "status": row["status"],
                "total_requests": int(row["total_requests"] or 0)
            }
            for row in status_rows
        ]

        return {
            "total_requests": total_requests,
            "sla_compliance_rate": float(sla_compliance_rate),
            "sla_breach_rate": float(sla_breach_rate),
            "sla_rate": float(sla_compliance_rate),
            "avg_resolution_time": avg_resolution_time,
            "median_resolution_time": median_resolution_time,
            "p90_resolution_time": p90_resolution_time,
            "backlog_ratio": float(backlog_ratio),
            "ses_score": float(ses),
            "ses_band": ses_band,
            "borough_stats": borough_stats,
            "sla_breakdown": top_by_volume,
            "sla_breakdown_total_types": len(sla_breakdown),
            "complaint_type_impact_top": top_by_impact,
            "status_breakdown": status_breakdown
        }