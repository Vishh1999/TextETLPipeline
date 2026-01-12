from pyspark.sql.functions import (col, lower, trim, translate, row_number,
                                   collect_list, first, lit, expr)
from pyspark.sql.window import Window


def basic_preprocessing(df, columns_to_select, column_to_standardise, new_column_name):
    """
    Select relevant columns and standardise a text column for joins.

    Parameters:
    df : DataFrame
        Input Spark DataFrame.
    columns_to_select : list[str]
        Columns to retain in the output DataFrame.
    column_to_standardise : str
        Column whose values should be standardised.
    new_column_name : str
        Name of the new standardised column.

    Returns:
    DataFrame
        Preprocessed Spark DataFrame.
    """
    # used to remove unwanted columns
    df_selected = df.select(*columns_to_select)

    # FIX for some bad records in the pageviews data (Ex: """Awaken""")
    if 'views' in df_selected.columns:
        df_selected = df_selected.filter(col("views").rlike("^[0-9]+$"))

    df_standardised = (
        df_selected
        # Convert text to lowercase
        .withColumn(
            new_column_name,
            lower(col(column_to_standardise))
        )
        # Replace underscores with spaces
        .withColumn(
            new_column_name,
            translate(col(new_column_name), "_", " ")
        )
        # Remove common punctuation
        .withColumn(
            new_column_name,
            translate(col(new_column_name), ":,.!?\"'()[]{}\\/ -", "")
        )
        # Trim leading/trailing whitespace
        .withColumn(
            new_column_name,
            trim(col(new_column_name))
        )
    )

    return df_standardised


def deduplicate_df(df, subset_columns, order_by_column=None, keep="first"):
    """
    Remove duplicate records from a Spark DataFrame.

    Parameters:
    df : DataFrame
        Input Spark DataFrame.
    subset_columns : list[str]
        Column(s) used to identify duplicates.
    order_by_column : str, optional
        Column used to decide which record to keep
        If None, Spark's default dropDuplicates is used.
    keep : str, optional
        Which record to keep when order_by_column is provided:
        'first' or 'last'. Default is 'first'.

    Returns:
    DataFrame
        Deduplicated Spark DataFrame.
    """
    # Simple deduplication (fast path)
    if order_by_column is None:
        return df.dropDuplicates(subset_columns)

    # Ordered deduplication (deterministic)
    ordering = col(order_by_column).asc() if keep == "first" else col(order_by_column).desc()

    window_spec = Window.partitionBy(*subset_columns).orderBy(ordering)

    return (
        df
        .withColumn("_row_number", row_number().over(window_spec))
        .filter(col("_row_number") == 1)
        .drop("_row_number")
    )


def df_join(left_df, right_df, join_columns, join_type="left"):
    """
    Join two Spark DataFrames on a specified column.

    Parameters:
    left_df : DataFrame
        Left DataFrame.
    right_df : DataFrame
        Right DataFrame.
    join_columns : list[str]
        List of Column names to join on (must exist in both DataFrames).
    join_type : str, optional
        Type of join (default is 'left').

    Returns:
    DataFrame
        Joined Spark DataFrame.
    """
    # Validate join columns exist in both DataFrames
    for col_name in join_columns:
        if col_name not in left_df.columns:
            raise ValueError(f"Column '{col_name}' not found in left DataFrame")
        if col_name not in right_df.columns:
            raise ValueError(f"Column '{col_name}' not found in right DataFrame")

    return left_df.join(
        right_df,
        on=join_columns,
        how=join_type
    )


def groupby_expr(df, group_col, expr_list):
    """
    Group a Spark DataFrame by a specified column and aggregate
    multiple expressions.

    Parameters:
    df : DataFrame
        Input Spark DataFrame.
    group_col : str
        Column to group by.
    expr_list : list[str]
        List of Spark SQL aggregation expressions as strings.

    Returns:
    DataFrame
        Grouped Spark DataFrame with aggregations.
    """
    # Group by the specified column and apply aggregations
    return df.groupBy(group_col).agg(*[expr(e) for e in expr_list])


def add_column(df, column_name, expression=None, default_value=None):
    """
    Add a column to a Spark DataFrame.

    Parameters:
    df : DataFrame
        Input Spark DataFrame.
    column_name : str
        Name of the column to add.
    expression : Column, optional
        Spark expression used to compute the column.
    default_value : any, optional
        Constant value to fill the column with if no expression is provided.

    Returns:
    DataFrame
        DataFrame with the new column added.
    """

    if isinstance(expression, str):
        return df.withColumn(column_name, expr(expression))

    if expression is not None:
        return df.withColumn(column_name, expression)

    if default_value is not None:
        return df.withColumn(column_name, lit(default_value))

    return df.withColumn(column_name, lit(None))
