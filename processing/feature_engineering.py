from pyspark.sql.functions import col, to_timestamp, datediff, when

def add_time_features(df):

    df = df.withColumn(
        "created_ts", to_timestamp(col("created_date"))
    ).withColumn(
        "closed_ts", to_timestamp(col("closed_date"))
    )

    return df


def add_resolution_metrics(df):

    df = df.withColumn(
        "resolution_time_days",
        datediff(col("closed_ts"), col("created_ts"))
    )

    df = df.withColumn(
        "resolution_completed",
        when(col("closed_ts").isNotNull(), 1).otherwise(0)
    )

    return df