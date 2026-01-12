def ingest_data(spark, filepath, filetype):
    """
    Ingest data into a Spark DataFrame from a file or directory.

    Parameters:
    spark : SparkSession
        Active Spark session.
    filepath : str
        Path to file or directory.
    filetype : str
        One of: 'csv', 'json', 'jsonl'

    Returns:
    DataFrame
        Spark DataFrame containing the ingested data.
    """

    filetype = filetype.lower()

    if filetype not in {"csv", "json", "jsonl"}:
        raise ValueError(
            f"Unsupported filetype '{filetype}'. "
            "Supported types: csv, json, jsonl"
        )

    reader = spark.read

    if filetype == "csv":
        df = (
            reader
            .option("header", True)
            .option("inferSchema", True)
            .csv(filepath)
        )

    elif filetype == "json":
        df = (
            reader
            .option("multiLine", True)
            .json(filepath)
        )

    elif filetype == "jsonl":
        df = (
            reader
            .option("multiLine", False)
            .json(filepath)
        )

    return df
