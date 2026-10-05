from pyspark.sql.functions import col, datediff, expr, lit, when

from core.config import settings


def _parse_timestamp_with_formats(column_name: str):
    escaped_formats = [fmt.replace("'", "''") for fmt in settings.TIMESTAMP_FORMATS]
    if not escaped_formats:
        return expr(f"try_to_timestamp(`{column_name}`)")
    return expr(
        "coalesce("
        + ", ".join([f"try_to_timestamp(`{column_name}`, '{fmt}')" for fmt in escaped_formats])
        + ")"
    )


def _parse_failed_flag(raw_column: str, parsed_column: str):
    return when(col(raw_column).isNotNull() & col(parsed_column).isNull(), lit(1)).otherwise(lit(0))


def add_time_features(df):
    df = (
        df.withColumn("created_ts", _parse_timestamp_with_formats("created_date"))
        .withColumn("due_ts", _parse_timestamp_with_formats("due_date"))
        .withColumn("closed_ts", _parse_timestamp_with_formats("closed_date"))
        .withColumn("created_ts_parse_failed", _parse_failed_flag("created_date", "created_ts"))
        .withColumn("due_ts_parse_failed", _parse_failed_flag("due_date", "due_ts"))
        .withColumn("closed_ts_parse_failed", _parse_failed_flag("closed_date", "closed_ts"))
    )

    return df


def _add_sla_breach(df):
    """Derive sla_breach from created_ts, due_ts, and closed_ts.

    Rule:
    - return null when any timestamp is missing
    - return null when the timestamp order is invalid
      (due or closed occurs before created)
    - return 1 when the request was closed after the due timestamp
    - return 0 when the request was closed on or before the due timestamp
    """
    all_timestamps_present = (
        col("created_ts").isNotNull() & col("due_ts").isNotNull() & col("closed_ts").isNotNull()
    )

    invalid_order = (col("due_ts") < col("created_ts")) | (col("closed_ts") < col("created_ts"))

    return df.withColumn(
        "sla_breach",
        when(~all_timestamps_present, lit(None).cast("int"))
        .when(invalid_order, lit(None).cast("int"))
        .when(col("closed_ts") > col("due_ts"), lit(1))
        .otherwise(lit(0)),
    )


def add_resolution_metrics(df):
    df = df.withColumn("resolution_time_days", datediff(col("closed_ts"), col("created_ts")))

    df = df.withColumn("resolution_completed", when(col("closed_ts").isNotNull(), 1).otherwise(0))

    df = _add_sla_breach(df)

    return df
