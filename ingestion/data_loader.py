def load_data(spark, file_path: str, file_type: str = "json"):
    if file_type == "json":
        # Read JSON files robustly: support both newline-delimited JSON and
        # JSON arrays by enabling multiline parsing.
        return spark.read.option("multiline", "true").json(file_path)

    else:
        raise ValueError("Unsupported file type")
