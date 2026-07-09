from pyspark.sql.functions import col

def clean_data(df):

    df = df.filter(col("created_date").isNotNull())

    df = df.dropDuplicates(["unique_key"])

    return df