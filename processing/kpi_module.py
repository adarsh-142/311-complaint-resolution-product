from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    avg,
    coalesce,
    col,
    count,
    current_date,
    datediff,
    desc,
    expr,
    lit,
    to_date,
    when,
)
from pyspark.sql.functions import (
    sum as spark_sum,
)

from core.config import settings
from processing.spark_operations import safe_collect


def _to_float(value: Any, default: float = 0.0) -> float:
    return float(value) if value is not None else float(default)


def _to_int(value: Any, default: int = 0) -> int:
    return int(value) if value is not None else int(default)


def _ses_band(ses_score: float) -> str:
    if ses_score >= 0.75:
        return "healthy"
    if ses_score >= 0.60:
        return "watch"
    if ses_score >= 0.45:
        return "needs_attention"
    return "critical"


def _collect_rows(df: DataFrame, description: str, timeout_seconds: int = 600):
    try:
        return safe_collect(df, timeout_seconds=timeout_seconds, description=description)
    except Exception:
        return []


def compute_operational_kpis(
    df: DataFrame,
    top_n: int = 10,
    trend_points: int = 30,
    agency_category_limit: int = 25,
) -> dict[str, Any]:
    """Compute bounded KPI outputs for 311 operations using Spark aggregations.

    This function only collects bounded aggregate outputs for API-safe payloads.
    """

    def _try_parse_expr(column_name: str):
        escaped_formats = [fmt.replace("'", "''") for fmt in settings.TIMESTAMP_FORMATS]
        if not escaped_formats:
            return expr(f"try_to_timestamp(`{column_name}`)")
        return expr(
            "coalesce("
            + ", ".join([f"try_to_timestamp(`{column_name}`, '{fmt}')" for fmt in escaped_formats])
            + ")"
        )

    working_df = df
    columns = set(working_df.columns)
    has_created_ts = "created_ts" in columns
    has_closed_ts = "closed_ts" in columns
    has_created_date = "created_date" in columns
    has_closed_date = "closed_date" in columns

    if not has_created_ts and has_created_date:
        created_expr = _try_parse_expr("created_date")
        working_df = working_df.withColumn("created_ts", created_expr)
    elif not has_created_ts:
        working_df = working_df.withColumn("created_ts", lit(None).cast("timestamp"))

    if not has_closed_ts and has_closed_date:
        closed_expr = _try_parse_expr("closed_date")
        working_df = working_df.withColumn("closed_ts", closed_expr)
    elif not has_closed_ts:
        working_df = working_df.withColumn("closed_ts", lit(None).cast("timestamp"))

    if "resolution_time_days" not in working_df.columns:
        working_df = working_df.withColumn(
            "resolution_time_days",
            datediff(col("closed_ts"), col("created_ts")),
        )
    if "sla_breach" not in working_df.columns:
        working_df = working_df.withColumn("sla_breach", lit(None).cast("int"))

    working_df = (
        working_df.withColumn(
            "resolution_time_days", greatest_non_negative(col("resolution_time_days"))
        )
        .withColumn("sla_breach_int", col("sla_breach").cast("int"))
        .withColumn("is_open", when(col("closed_ts").isNull(), lit(1)).otherwise(lit(0)))
        .withColumn("is_closed", when(col("closed_ts").isNotNull(), lit(1)).otherwise(lit(0)))
        .withColumn("created_day", to_date(col("created_ts")))
        .withColumn(
            "age_days",
            when(col("created_ts").isNull(), lit(None)).otherwise(
                datediff(current_date(), to_date(col("created_ts")))
            ),
        )
    )

    summary_row = _collect_rows(
        working_df.agg(
            count("*").alias("total_requests"),
            spark_sum("is_open").alias("open_requests"),
            spark_sum("is_closed").alias("closed_requests"),
            avg("resolution_time_days").alias("avg_resolution_time"),
            expr("percentile_approx(resolution_time_days, 0.5)").alias("median_resolution_time"),
            expr("percentile_approx(resolution_time_days, 0.9)").alias("p90_resolution_time"),
            spark_sum(when(col("sla_breach_int") == 1, 1).otherwise(0)).alias("sla_breach_count"),
            spark_sum(when(col("sla_breach_int").isNotNull(), 1).otherwise(0)).alias(
                "sla_observed_count"
            ),
        ),
        description="kpi summary",
    )[0]

    total_requests = _to_int(summary_row["total_requests"])
    open_requests = _to_int(summary_row["open_requests"])
    closed_requests = _to_int(summary_row["closed_requests"])
    avg_resolution_time = _to_float(summary_row["avg_resolution_time"])
    median_resolution_time = _to_float(summary_row["median_resolution_time"])
    p90_resolution_time = _to_float(summary_row["p90_resolution_time"])
    sla_breach_count = _to_int(summary_row["sla_breach_count"])
    sla_observed_count = _to_int(summary_row["sla_observed_count"])

    open_rate = (open_requests / total_requests) if total_requests else 0.0
    closed_rate = (closed_requests / total_requests) if total_requests else 0.0
    backlog_ratio = open_rate
    sla_breach_rate = (sla_breach_count / sla_observed_count) if sla_observed_count else 0.0
    sla_compliance_rate = 1.0 - sla_breach_rate

    ses_score = (
        (1 - backlog_ratio) * 0.3
        + sla_compliance_rate * 0.4
        + (1 / (1 + avg_resolution_time)) * 0.3
    )

    trend_rows = _collect_rows(
        working_df.filter(col("created_day").isNotNull())
        .groupBy("created_day")
        .agg(
            count("*").alias("volume"),
            spark_sum("is_open").alias("open_requests"),
            spark_sum("is_closed").alias("closed_requests"),
            avg("resolution_time_days").alias("avg_resolution_time"),
            avg(col("sla_breach_int")).alias("sla_breach_rate"),
        )
        .orderBy(desc("created_day"))
        .limit(trend_points),
        description="time trends",
    )

    time_trends = [
        {
            "date": str(row["created_day"]),
            "volume": _to_int(row["volume"]),
            "open_requests": _to_int(row["open_requests"]),
            "closed_requests": _to_int(row["closed_requests"]),
            "avg_resolution_time": _to_float(row["avg_resolution_time"]),
            "sla_breach_rate": _to_float(row["sla_breach_rate"]),
        }
        for row in sorted(trend_rows, key=lambda item: str(item["created_day"]))
    ]

    complaint_rows = []
    if "complaint_type" in working_df.columns:
        complaint_rows = _collect_rows(
            working_df.groupBy(
                coalesce(col("complaint_type"), lit("Unknown")).alias("complaint_type")
            )
            .agg(
                count("*").alias("total_requests"),
                spark_sum("is_open").alias("open_requests"),
                spark_sum("is_closed").alias("closed_requests"),
                avg("resolution_time_days").alias("avg_resolution_time"),
                expr("percentile_approx(resolution_time_days, 0.5)").alias(
                    "median_resolution_time"
                ),
                avg(col("sla_breach_int")).alias("sla_breach_rate"),
            )
            .orderBy(desc("total_requests"), col("complaint_type").asc())
            .limit(top_n),
            description="top complaint types",
        )

    top_complaint_types = []
    for row in complaint_rows:
        total = _to_int(row["total_requests"])
        breach_rate = _to_float(row["sla_breach_rate"])
        avg_time = _to_float(row["avg_resolution_time"])
        volume_share = (total / total_requests) if total_requests else 0.0
        impact_score = volume_share * breach_rate * max(avg_time, 0.0)

        top_complaint_types.append(
            {
                "complaint_type": row["complaint_type"],
                "total_requests": total,
                "open_requests": _to_int(row["open_requests"]),
                "closed_requests": _to_int(row["closed_requests"]),
                "avg_resolution_time": avg_time,
                "median_resolution_time": _to_float(row["median_resolution_time"]),
                "sla_breach_rate": breach_rate,
                "sla_compliance_rate": 1.0 - breach_rate,
                "volume_share": float(volume_share),
                "impact_score": float(impact_score),
            }
        )

    agency_rows = []
    if "agency" in working_df.columns:
        agency_rows = _collect_rows(
            working_df.groupBy(coalesce(col("agency"), lit("Unknown")).alias("agency"))
            .agg(
                count("*").alias("total_requests"),
                spark_sum("is_open").alias("open_requests"),
                spark_sum("is_closed").alias("closed_requests"),
                avg("resolution_time_days").alias("avg_resolution_time"),
                expr("percentile_approx(resolution_time_days, 0.5)").alias(
                    "median_resolution_time"
                ),
                avg(col("sla_breach_int")).alias("sla_breach_rate"),
            )
            .orderBy(desc("total_requests"), col("agency").asc())
            .limit(top_n),
            description="top agencies",
        )

    top_agencies = [
        {
            "agency": row["agency"],
            "total_requests": _to_int(row["total_requests"]),
            "open_requests": _to_int(row["open_requests"]),
            "closed_requests": _to_int(row["closed_requests"]),
            "avg_resolution_time": _to_float(row["avg_resolution_time"]),
            "median_resolution_time": _to_float(row["median_resolution_time"]),
            "sla_breach_rate": _to_float(row["sla_breach_rate"]),
            "sla_compliance_rate": 1.0 - _to_float(row["sla_breach_rate"]),
        }
        for row in agency_rows
    ]

    borough_rows = []
    if "borough" in working_df.columns:
        borough_rows = _collect_rows(
            working_df.groupBy(coalesce(col("borough"), lit("Unknown")).alias("borough"))
            .agg(
                count("*").alias("total_requests"),
                spark_sum("is_open").alias("open_requests"),
                spark_sum("is_closed").alias("closed_requests"),
                avg("resolution_time_days").alias("avg_resolution_time"),
                avg(col("sla_breach_int")).alias("sla_breach_rate"),
            )
            .orderBy(desc("total_requests"), col("borough").asc())
            .limit(top_n),
            description="borough performance",
        )

    borough_performance = [
        {
            "borough": row["borough"],
            "total_requests": _to_int(row["total_requests"]),
            "open_requests": _to_int(row["open_requests"]),
            "closed_requests": _to_int(row["closed_requests"]),
            "open_rate": (_to_int(row["open_requests"]) / _to_int(row["total_requests"]))
            if _to_int(row["total_requests"])
            else 0.0,
            "avg_resolution_time": _to_float(row["avg_resolution_time"]),
            "sla_breach_rate": _to_float(row["sla_breach_rate"]),
            "sla_compliance_rate": 1.0 - _to_float(row["sla_breach_rate"]),
        }
        for row in borough_rows
    ]

    backlog_bucket_df = working_df.filter(col("is_open") == 1).withColumn(
        "age_bucket",
        when(col("age_days").isNull(), lit("unknown"))
        .when(col("age_days") <= 7, lit("0-7d"))
        .when(col("age_days") <= 30, lit("8-30d"))
        .when(col("age_days") <= 90, lit("31-90d"))
        .otherwise(lit("90d+")),
    )

    backlog_rows = _collect_rows(
        backlog_bucket_df.groupBy("age_bucket")
        .agg(count("*").alias("open_count"), avg("age_days").alias("avg_age_days"))
        .orderBy(
            expr(
                "CASE age_bucket "
                "WHEN '0-7d' THEN 1 "
                "WHEN '8-30d' THEN 2 "
                "WHEN '31-90d' THEN 3 "
                "WHEN '90d+' THEN 4 "
                "ELSE 5 END"
            )
        ),
        description="backlog ageing",
    )

    backlog_ageing = [
        {
            "age_bucket": row["age_bucket"],
            "open_count": _to_int(row["open_count"]),
            "avg_age_days": _to_float(row["avg_age_days"]),
        }
        for row in backlog_rows
    ]

    agency_category_rows = []
    if "agency" in working_df.columns and "complaint_type" in working_df.columns:
        agency_category_rows = _collect_rows(
            working_df.groupBy(
                coalesce(col("agency"), lit("Unknown")).alias("agency"),
                coalesce(col("complaint_type"), lit("Unknown")).alias("complaint_type"),
            )
            .agg(
                count("*").alias("total_requests"),
                spark_sum("is_open").alias("open_requests"),
                spark_sum("is_closed").alias("closed_requests"),
                avg("resolution_time_days").alias("avg_resolution_time"),
                expr("percentile_approx(resolution_time_days, 0.5)").alias(
                    "median_resolution_time"
                ),
                avg(col("sla_breach_int")).alias("sla_breach_rate"),
            )
            .orderBy(desc("total_requests"), col("agency").asc(), col("complaint_type").asc())
            .limit(agency_category_limit),
            description="agency category performance",
        )

    agency_category_performance = [
        {
            "agency": row["agency"],
            "complaint_type": row["complaint_type"],
            "total_requests": _to_int(row["total_requests"]),
            "open_requests": _to_int(row["open_requests"]),
            "closed_requests": _to_int(row["closed_requests"]),
            "closed_rate": (_to_int(row["closed_requests"]) / _to_int(row["total_requests"]))
            if _to_int(row["total_requests"])
            else 0.0,
            "avg_resolution_time": _to_float(row["avg_resolution_time"]),
            "median_resolution_time": _to_float(row["median_resolution_time"]),
            "sla_breach_rate": _to_float(row["sla_breach_rate"]),
            "sla_compliance_rate": 1.0 - _to_float(row["sla_breach_rate"]),
        }
        for row in agency_category_rows
    ]

    status_rows = []
    if "status" in working_df.columns:
        status_rows = _collect_rows(
            working_df.groupBy(coalesce(col("status"), lit("Unknown")).alias("status"))
            .agg(count("*").alias("total_requests"))
            .orderBy(desc("total_requests"), col("status").asc())
            .limit(top_n),
            description="status distribution",
        )

    status_breakdown = [
        {
            "status": row["status"],
            "total_requests": _to_int(row["total_requests"]),
        }
        for row in status_rows
    ]

    complaint_type_impact_top = sorted(
        top_complaint_types,
        key=lambda item: item["impact_score"],
        reverse=True,
    )[:10]

    return {
        "total_requests": total_requests,
        "open_requests": open_requests,
        "closed_requests": closed_requests,
        "open_rate": float(open_rate),
        "closed_rate": float(closed_rate),
        "backlog_ratio": float(backlog_ratio),
        "avg_resolution_time": float(avg_resolution_time),
        "median_resolution_time": float(median_resolution_time),
        "p90_resolution_time": float(p90_resolution_time),
        "sla_breach_count": sla_breach_count,
        "sla_breach_rate": float(sla_breach_rate),
        "sla_compliance_rate": float(sla_compliance_rate),
        "sla_rate": float(sla_compliance_rate),
        "ses_score": float(ses_score),
        "ses_band": _ses_band(float(ses_score)),
        "volume": {
            "total_requests": total_requests,
            "open_requests": open_requests,
            "closed_requests": closed_requests,
            "open_rate": float(open_rate),
            "closed_rate": float(closed_rate),
        },
        "resolution": {
            "avg_resolution_time": float(avg_resolution_time),
            "median_resolution_time": float(median_resolution_time),
            "p90_resolution_time": float(p90_resolution_time),
        },
        "sla": {
            "breach_count": sla_breach_count,
            "breach_rate": float(sla_breach_rate),
            "compliance_rate": float(sla_compliance_rate),
            "observed_count": sla_observed_count,
        },
        "time_trends": time_trends,
        "top_complaint_types": top_complaint_types,
        "top_agencies": top_agencies,
        "borough_performance": borough_performance,
        "borough_stats": borough_performance,
        "backlog_ageing": backlog_ageing,
        "agency_category_performance": agency_category_performance,
        "complaint_type_impact_top": complaint_type_impact_top,
        "status_breakdown": status_breakdown,
    }


def greatest_non_negative(column_expr):
    return when(column_expr.isNull(), None).otherwise(
        when(column_expr < 0, 0).otherwise(column_expr)
    )
