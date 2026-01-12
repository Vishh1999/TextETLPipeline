import random
from pyspark.sql import SparkSession
from config import (DATA_PATH, MONGO_DB_NAME, MONGO_COLLECTION_NAME, MONGO_URI,
                    BATCH_SIZE, POSTGRES_DB_CONFIG, POSTGRES_MAIN_TABLE_NAME,
                    POSTGRES_ANALYTICS_TABLE_NAME)
from src.extract import ingest_data
from src.transform import basic_preprocessing, deduplicate_df, df_join, groupby_expr, add_column
from src.load_mongo import upload_to_mongo
from src.load_postgresql import load_to_postgres

# -- EXTRACT PHASE --
spark = SparkSession.builder.getOrCreate()
pageviews_data = ingest_data(spark, f"{DATA_PATH}/pageviews.csv", filetype="csv")
print(f"Pageviews data row count: {pageviews_data.count()}")
pageviews_data.printSchema()
# sample of pageviews data
pageviews_data.show(5, truncate=False)
print(f"Number of Spark partitions in pageviews data: {pageviews_data.rdd.getNumPartitions()}")

articles_data = ingest_data(spark, f"{DATA_PATH}/enwiki_namespace_0", filetype="jsonl")
print(f"Articles' data row count: {articles_data.count()}")
articles_data.printSchema()
# sample of articles data
articles_data.show(5, truncate=False)
print(f"Number of Spark partitions in articles data: {articles_data.rdd.getNumPartitions()}")

# -- TRANSFORM PHASE --
# Preprocess pageviews data with creation of cleaned, standardised "article_name" column
pageviews_df = basic_preprocessing(
    df=pageviews_data,
    columns_to_select=["article", "views", "date", "rank"],
    column_to_standardise="article",
    new_column_name="article_name"
)
pageviews_df.select("article", "article_name").show(5, truncate=False)

# Preprocess articles data with creation of cleaned, standardised "article_name" column
articles_df = basic_preprocessing(
    df=articles_data,
    columns_to_select=["name", "description", "url", "abstract"],
    column_to_standardise="name",
    new_column_name="article_name"
)
articles_df.select("name", "article_name").show(5, truncate=False)

# Keep only unique records for each article name
print(f"Before dedup: {articles_df.count()}")
articles_df = deduplicate_df(df=articles_df, subset_columns=["article_name"])
print(f"After dedup: {articles_df.count()}")

# Join the 2 dfs on "article_name" column
df = df_join(
    left_df=pageviews_df,
    right_df=articles_df,
    join_columns=["article_name"],
    join_type="left"
)
print(f"Joined df Row count: {df.count()}")
df.printSchema()
df.show(5, truncate=False)

print(f"Check number of unique articles present in pageviews data: "
      f"{df.select("article_name").distinct().count()}")
# pre-sort the data based on article_name and date
mongo_loading_df = df.orderBy("article_name", "date")

# This groups on article_name, converts pageviews data into lists and
# also conveniently, removes older article and name columns
list_collection_expr = [
    "COLLECT_LIST(date) AS date",
    "COLLECT_LIST(views) AS views",
    "COLLECT_LIST(rank) AS rank",
    "FIRST(description) AS description",
    "FIRST(url) AS url",
    "FIRST(abstract) AS abstract"
]
mongo_loading_df = groupby_expr(df=mongo_loading_df,
                                group_col="article_name",
                                expr_list=list_collection_expr)
print(f"Check number of articles present in flattened data: {mongo_loading_df.count()}")
mongo_loading_df.show(5, truncate=False)

# simple transformation, adding one column 'popularity_bucket' based on views column
popularity_expr = """
CASE
    WHEN CAST(views AS BIGINT) < 10000 THEN 'low'
    WHEN CAST(views AS BIGINT) < 100000 THEN 'medium'
    ELSE 'high'
END
"""
pageviews_df = add_column(df=pageviews_df,
                          column_name='popularity_bucket',
                          expression=popularity_expr)
pageviews_df.show(5, truncate=False)

analytics_expr_list = [
    "AVG(CAST(views AS BIGINT)) AS avg_views",
    "MAX(CAST(views AS BIGINT)) AS max_views",
    "MIN(rank) AS best_rank",
    "COUNT(DISTINCT date) AS total_days_ranked"
]
analytics_df = groupby_expr(df=pageviews_df, group_col="article_name", expr_list=analytics_expr_list)
analytics_df.show(5, truncate=False)

# -- LOAD PHASE --
# MongoDB Loading
# Convert Spark rows to Python dictionaries for upload to mongoDB
data_to_insert_mongo = mongo_loading_df.toPandas().to_dict('records')
print(f"Sample insert document to MongoDB: "
      f"{data_to_insert_mongo[random.randint(0, len(data_to_insert_mongo) - 1)]}")
upload_to_mongo(data_to_insert_mongo, MONGO_DB_NAME, MONGO_COLLECTION_NAME, MONGO_URI, BATCH_SIZE)

# PostgreSQL Loading
postgres_records = (
    pageviews_df
    .select('article_name', 'views', 'popularity_bucket', 'date', 'rank')
    .toPandas()
    .itertuples(index=False, name=None)
)

columns = ["article_name", "views", "popularity_bucket", "date", "rank"]
column_types = {
    "article_name": "TEXT",
    "views": "BIGINT",
    "popularity_bucket": "TEXT",
    "date": "DATE",
    "rank": "INT"
}
load_to_postgres(
    records=postgres_records,
    columns=columns,
    column_types=column_types,
    db_config=POSTGRES_DB_CONFIG,
    table_name=POSTGRES_MAIN_TABLE_NAME,
)

load_to_postgres(
    records=analytics_df.toPandas().itertuples(index=False, name=None),
                 columns=["article_name", "avg_views", "max_views", "best_rank", "total_days_ranked"],
                 column_types={
                     "article_name": "TEXT",
                     "avg_views": "FLOAT",
                     "max_views": "FLOAT",
                     "best_rank": "INT",
                     "total_days_ranked": "INT"
                 },
                 db_config=POSTGRES_DB_CONFIG,
                 table_name=POSTGRES_ANALYTICS_TABLE_NAME
)
