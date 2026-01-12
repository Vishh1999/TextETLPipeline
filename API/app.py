from fastapi import FastAPI, Query
from pymongo import MongoClient
import psycopg2
from config import POSTGRES_DB_CONFIG, MONGO_DB_NAME, MONGO_URI, MONGO_COLLECTION_NAME

# App initialisation
app = FastAPI(
    title="Wikipedia Pageviews API",
    description="Analytics and content APIs built on Wikipedia pageview data",
    version="1.0.0"
)

# Database connections
def get_postgres_connection():
    return psycopg2.connect(**POSTGRES_DB_CONFIG)

mongo_client = MongoClient(MONGO_URI)
mongo_collection = mongo_client[MONGO_DB_NAME][MONGO_COLLECTION_NAME]


# API 1: Most popular article by DAY
@app.get("/analytics/top-article/day")
def most_popular_article_day(
    date: str = Query(..., example="2017-09-21")
):
    query = """
        SELECT article_name, views, rank
        FROM article_rankings
        WHERE date = %s
        AND rank = 1
    """
    with get_postgres_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (date,))
            row = cur.fetchone()

    if not row:
        return {"message": "No data found for the given date"}

    return {
        "article_name": row[0],
        "views": row[1],
        "rank": row[2]
    }


# API 2: Most popular article by MONTH
@app.get("/analytics/top-article/month")
def most_popular_article_month(
    year: int = Query(..., example=2017),
    month: int = Query(..., example=9)
):
    query = """
        SELECT article_name, SUM(views) AS total_views
        FROM article_rankings
        WHERE EXTRACT(YEAR FROM date) = %s
          AND EXTRACT(MONTH FROM date) = %s
        GROUP BY article_name
        ORDER BY total_views DESC
        LIMIT 1;
    """
    with get_postgres_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (year, month))
            row = cur.fetchone()

    if not row:
        return {"message": "No data found for the given month"}

    return {
        "year": year,
        "month": month,
        "article_name": row[0],
        "total_views": row[1]
    }


# API 3: Most popular article by YEAR
@app.get("/analytics/top-article/year")
def most_popular_article_year(
    year: int = Query(..., example=2017)
):
    query = """
        SELECT article_name, SUM(views) AS total_views
        FROM article_rankings
        WHERE EXTRACT(YEAR FROM date) = %s
        GROUP BY article_name
        ORDER BY total_views DESC
        LIMIT 1;
    """
    with get_postgres_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (year,))
            row = cur.fetchone()

    if not row:
        return {"message": "No data found for the given year"}

    return {
        "year": year,
        "article_name": row[0],
        "total_views": row[1]
    }


# API 4: Full article data (MongoDB)
@app.get("/articles/{article_name}")
def get_full_article(article_name: str):
    document = mongo_collection.find_one({"article_name": article_name}, {"_id": 0})

    if not document:
        return {"message": "Article not found"}

    return document


# API 5: Key analytics stat per article
@app.get("/analytics/article/{article_name}")
def get_article_analytics(article_name: str):
    query = """
        SELECT
            article_name,
            avg_views,
            max_views,
            best_rank,
            total_days_ranked
        FROM article_analytics
        WHERE article_name = %s;
    """
    with get_postgres_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (article_name,))
            row = cur.fetchone()

    if not row:
        return {"message": "No analytics data found for this article"}

    return {
        "article_name": row[0],
        "avg_views": row[1],
        "max_views": row[2],
        "best_rank": row[3],
        "total_days_tracked": row[4]
    }
