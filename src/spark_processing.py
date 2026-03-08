"""
Spark batch processing and SQL analytics for e-commerce data (Part 2).

Covers data cleaning/normalization, product recommendations ("users who
bought X also bought Y"), cohort analysis (registration-month cohorts with
spending retention), and five substantive Spark SQL queries.

Usage: python src/spark_processing.py
"""

import os
from itertools import combinations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")

SEPARATOR = "=" * 80


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def banner(title: str) -> None:
    """Print a visible section header."""
    print(f"\n{SEPARATOR}")
    print(f"  {title}")
    print(SEPARATOR)


# ---------------------------------------------------------------------------
# Section 1 — Data Loading & Cleaning
# ---------------------------------------------------------------------------
def load_and_clean(spark: SparkSession) -> dict:
    """Load all JSON datasets, clean, and return a dict of DataFrames."""
    banner("SECTION 1: Data Loading & Cleaning")

    # --- Load raw ---
    # JSON files are pretty-printed (multi-line arrays), so multiLine=True is required.
    users_df = spark.read.option("multiLine", True).json(os.path.join(DATA_DIR, "users.json"))
    products_df = spark.read.option("multiLine", True).json(os.path.join(DATA_DIR, "products.json"))
    categories_df = spark.read.option("multiLine", True).json(os.path.join(DATA_DIR, "categories.json"))
    transactions_df = spark.read.option("multiLine", True).json(os.path.join(DATA_DIR, "transactions.json"))

    # Sessions are split across multiple files — load each and union
    import glob as _glob
    session_files = sorted(_glob.glob(os.path.join(DATA_DIR, "sessions_*.json")))
    sessions_df = spark.read.option("multiLine", True).json(session_files)

    print("\n--- Raw record counts ---")
    for name, df in [
        ("users", users_df),
        ("products", products_df),
        ("categories", categories_df),
        ("sessions", sessions_df),
        ("transactions", transactions_df),
    ]:
        print(f"  {name:>15s}: {df.count():>6,} rows")

    # --- Schema inspection ---
    print("\n--- Schemas ---")
    for name, df in [
        ("users", users_df),
        ("products", products_df),
        ("sessions", sessions_df),
        ("transactions", transactions_df),
    ]:
        print(f"\n[{name}]")
        df.printSchema()

    # --- Data quality report ---
    print("\n--- Data quality: null counts per column ---")
    for name, df in [
        ("users", users_df),
        ("products", products_df),
        ("sessions", sessions_df),
        ("transactions", transactions_df),
    ]:
        null_counts = []
        for col_name in df.columns:
            n = df.filter(F.col(col_name).isNull()).count()
            if n > 0:
                null_counts.append((col_name, n))
        if null_counts:
            print(f"\n  [{name}]")
            for col_name, n in null_counts:
                print(f"    {col_name}: {n}")
        else:
            print(f"\n  [{name}] — no nulls")

    # --- Duplicate check ---
    print("\n--- Duplicate primary key check ---")
    for name, df, pk in [
        ("users", users_df, "user_id"),
        ("products", products_df, "product_id"),
        ("sessions", sessions_df, "session_id"),
        ("transactions", transactions_df, "transaction_id"),
    ]:
        total = df.count()
        distinct = df.select(pk).distinct().count()
        dupes = total - distinct
        status = "OK" if dupes == 0 else f"{dupes} duplicates!"
        print(f"  {name:>15s}: {status}")

    # --- Clean & normalize ---

    # Users: cast dates to timestamps
    users_df = users_df.withColumn(
        "registration_date", F.to_timestamp("registration_date")
    ).withColumn("last_active", F.to_timestamp("last_active"))

    # Products: cast dates, fill null stock with 0
    products_df = (
        products_df.withColumn("creation_date", F.to_timestamp("creation_date"))
        .withColumn("current_stock", F.coalesce(F.col("current_stock"), F.lit(0)))
        .withColumn(
            "is_active",
            F.coalesce(F.col("is_active"), F.lit(True)),
        )
    )

    # Categories: explode subcategories for a flat lookup
    subcategories_df = categories_df.select(
        F.col("category_id"),
        F.col("name").alias("category_name"),
        F.explode("subcategories").alias("sub"),
    ).select(
        "category_id",
        "category_name",
        F.col("sub.subcategory_id").alias("subcategory_id"),
        F.col("sub.name").alias("subcategory_name"),
        F.col("sub.profit_margin").alias("profit_margin"),
    )

    # Sessions: cast timestamps, handle null page_view fields
    sessions_df = sessions_df.withColumn(
        "start_time", F.to_timestamp("start_time")
    ).withColumn("end_time", F.to_timestamp("end_time"))

    # Transactions: cast timestamp
    transactions_df = transactions_df.withColumn(
        "timestamp", F.to_timestamp("timestamp")
    )

    print("\n--- Cleaning complete ---")
    print("  • Dates cast to TimestampType")
    print("  • Null current_stock filled with 0")
    print("  • Null is_active filled with True")
    print("  • Subcategories exploded into flat lookup table")

    return {
        "users": users_df,
        "products": products_df,
        "categories": categories_df,
        "subcategories": subcategories_df,
        "sessions": sessions_df,
        "transactions": transactions_df,
    }


# ---------------------------------------------------------------------------
# Section 2 — Product Recommendation ("also bought")
# ---------------------------------------------------------------------------
def product_recommendations(dfs: dict):
    """Build co-purchase pairs: 'users who bought X also bought Y'. Returns pd.DataFrame."""
    banner("SECTION 2: Product Recommendations — Co-Purchase Analysis")

    spark = dfs["transactions"].sparkSession
    transactions_df = dfs["transactions"]
    products_df = dfs["products"]

    # Explode items to get one row per (transaction_id, product_id)
    txn_items = transactions_df.select(
        "transaction_id",
        F.explode("items").alias("item"),
    ).select(
        "transaction_id",
        F.col("item.product_id").alias("product_id"),
    )

    # Self-join to find all product pairs within the same transaction
    pairs = (
        txn_items.alias("a")
        .join(txn_items.alias("b"), on="transaction_id")
        .filter(F.col("a.product_id") < F.col("b.product_id"))
        .select(
            F.col("a.product_id").alias("product_a"),
            F.col("b.product_id").alias("product_b"),
        )
    )

    # Count co-occurrences
    pair_counts = (
        pairs.groupBy("product_a", "product_b")
        .agg(F.count("*").alias("co_purchase_count"))
        .orderBy(F.desc("co_purchase_count"))
    )

    print("\n--- Top 20 most frequently co-purchased product pairs ---")
    pair_counts.join(
        products_df.select(
            F.col("product_id").alias("product_a"),
            F.col("name").alias("product_a_name"),
        ),
        on="product_a",
    ).join(
        products_df.select(
            F.col("product_id").alias("product_b"),
            F.col("name").alias("product_b_name"),
        ),
        on="product_b",
    ).select(
        "product_a",
        "product_a_name",
        "product_b",
        "product_b_name",
        "co_purchase_count",
    ).orderBy(
        F.desc("co_purchase_count")
    ).show(
        20, truncate=40
    )

    # For each product, find top-3 recommended products
    # Unpivot pairs so each product appears in the "source" column
    recs_a = pair_counts.select(
        F.col("product_a").alias("source_product"),
        F.col("product_b").alias("recommended_product"),
        "co_purchase_count",
    )
    recs_b = pair_counts.select(
        F.col("product_b").alias("source_product"),
        F.col("product_a").alias("recommended_product"),
        "co_purchase_count",
    )
    all_recs = recs_a.union(recs_b)

    w = Window.partitionBy("source_product").orderBy(F.desc("co_purchase_count"))
    top3 = (
        all_recs.withColumn("rank", F.row_number().over(w))
        .filter(F.col("rank") <= 3)
        .drop("rank")
    )

    # Show a sample (first 30 rows — 10 products × 3 recs each)
    print("\n--- Top-3 recommendations per product (sample) ---")
    top3.join(
        products_df.select(
            F.col("product_id").alias("source_product"),
            F.col("name").alias("source_name"),
        ),
        on="source_product",
    ).join(
        products_df.select(
            F.col("product_id").alias("recommended_product"),
            F.col("name").alias("rec_name"),
        ),
        on="recommended_product",
    ).select(
        "source_product",
        "source_name",
        "recommended_product",
        "rec_name",
        "co_purchase_count",
    ).orderBy(
        "source_product", F.desc("co_purchase_count")
    ).show(
        30, truncate=35
    )

    # Return enriched top-3 recs as pandas DataFrame
    result = top3.join(
        products_df.select(
            F.col("product_id").alias("source_product"),
            F.col("name").alias("source_name"),
        ),
        on="source_product",
    ).join(
        products_df.select(
            F.col("product_id").alias("recommended_product"),
            F.col("name").alias("rec_name"),
        ),
        on="recommended_product",
    ).select(
        "source_product", "source_name",
        "recommended_product", "rec_name",
        "co_purchase_count",
    ).orderBy("source_product", F.desc("co_purchase_count"))
    return result.toPandas()


# ---------------------------------------------------------------------------
# Section 3 — Cohort Analysis
# ---------------------------------------------------------------------------
def cohort_analysis(dfs: dict):
    """Group users by registration month, track monthly spending & retention. Returns (spending_pd, retention_pd)."""
    banner("SECTION 3: Cohort Analysis — Registration-Month Cohorts")

    users_df = dfs["users"]
    transactions_df = dfs["transactions"]

    # Assign each user to a registration cohort (year-month)
    users_cohort = users_df.select(
        "user_id",
        F.date_format("registration_date", "yyyy-MM").alias("cohort"),
    )

    # Get transaction month for each purchase
    txn_month = transactions_df.select(
        "user_id",
        "total",
        F.date_format("timestamp", "yyyy-MM").alias("txn_month"),
    )

    # Join to attach cohort
    cohort_txn = txn_month.join(users_cohort, on="user_id")

    # --- Cohort spending matrix ---
    # Calculate months since registration
    cohort_txn = cohort_txn.withColumn(
        "months_since_reg",
        F.months_between(
            F.to_date(F.col("txn_month"), "yyyy-MM"),
            F.to_date(F.col("cohort"), "yyyy-MM"),
        ).cast("int"),
    )

    spending_matrix = (
        cohort_txn.groupBy("cohort")
        .pivot("months_since_reg")
        .agg(F.round(F.sum("total"), 2))
        .orderBy("cohort")
    )

    print("\n--- Cohort spending matrix (total revenue by months since registration) ---")
    spending_matrix.show(50, truncate=False)

    # --- Cohort size & average spending ---
    cohort_summary = (
        cohort_txn.groupBy("cohort")
        .agg(
            F.countDistinct("user_id").alias("active_users"),
            F.count("*").alias("total_transactions"),
            F.round(F.sum("total"), 2).alias("total_revenue"),
            F.round(F.avg("total"), 2).alias("avg_order_value"),
        )
        .orderBy("cohort")
    )

    print("\n--- Cohort summary ---")
    cohort_summary.show(50, truncate=False)

    # --- Retention: unique users who transacted each month offset ---
    retention = (
        cohort_txn.groupBy("cohort")
        .pivot("months_since_reg")
        .agg(F.countDistinct("user_id"))
        .orderBy("cohort")
    )

    print("\n--- Cohort retention (unique users per month offset) ---")
    retention.show(50, truncate=False)

    # Compute retention rates as percentage of cohort size
    cohort_sizes = users_cohort.groupBy("cohort").agg(
        F.count("user_id").alias("cohort_size")
    )
    retention_pct = retention.join(cohort_sizes, on="cohort")

    # Get month-offset columns (everything except 'cohort' and 'cohort_size')
    month_cols = [
        c for c in retention_pct.columns if c not in ("cohort", "cohort_size")
    ]
    for mc in month_cols:
        retention_pct = retention_pct.withColumn(
            mc,
            F.round(F.col(mc) / F.col("cohort_size") * 100, 1),
        )

    print("\n--- Cohort retention rates (% of cohort) ---")
    retention_pct_ordered = retention_pct.orderBy("cohort")
    retention_pct_ordered.show(50, truncate=False)

    return spending_matrix.toPandas(), retention_pct_ordered.toPandas()


# ---------------------------------------------------------------------------
# Section 4 — Spark SQL Queries
# ---------------------------------------------------------------------------
def spark_sql_queries(dfs: dict):
    """Register temp views and run analytical SQL queries. Returns dict of pd.DataFrames."""
    banner("SECTION 4: Spark SQL Analytics")

    spark = dfs["users"].sparkSession

    # Register temp views
    dfs["users"].createOrReplaceTempView("users")
    dfs["products"].createOrReplaceTempView("products")
    dfs["categories"].createOrReplaceTempView("categories")
    dfs["subcategories"].createOrReplaceTempView("subcategories")
    dfs["sessions"].createOrReplaceTempView("sessions")
    dfs["transactions"].createOrReplaceTempView("transactions")

    # -- Query 1: Top revenue-generating products with category info ----------
    print("\n--- SQL Query 1: Top 15 Revenue-Generating Products ---")
    print("(Demonstrates joining transactions items with product & category data —")
    print(" data that would live in MongoDB's document store)\n")

    q1_df = spark.sql("""
        WITH txn_items AS (
            SELECT transaction_id, item.*
            FROM transactions
            LATERAL VIEW explode(items) t AS item
        )
        SELECT
            p.product_id,
            p.name               AS product_name,
            sc.category_name,
            sc.subcategory_name,
            sc.profit_margin,
            COUNT(*)             AS times_sold,
            SUM(ti.quantity)     AS units_sold,
            ROUND(SUM(ti.subtotal), 2)         AS total_revenue,
            ROUND(SUM(ti.subtotal) * sc.profit_margin, 2) AS est_profit
        FROM txn_items ti
        JOIN products p ON ti.product_id = p.product_id
        JOIN subcategories sc ON p.subcategory_id = sc.subcategory_id
        GROUP BY p.product_id, p.name, sc.category_name,
                 sc.subcategory_name, sc.profit_margin
        ORDER BY total_revenue DESC
        LIMIT 15
    """)
    q1_df.show(truncate=40)

    # -- Query 2: User session behaviour by device type -----------------------
    print("\n--- SQL Query 2: Session Behaviour by Device Type ---")
    print("(Analyses session data that would reside in HBase's time-series store)\n")

    q2_df = spark.sql("""
        SELECT
            device_profile.type  AS device_type,
            device_profile.os    AS os,
            COUNT(*)             AS session_count,
            ROUND(AVG(duration_seconds), 0)        AS avg_duration_sec,
            ROUND(AVG(SIZE(page_views)), 1)        AS avg_pages_per_session,
            ROUND(AVG(SIZE(viewed_products)), 1)   AS avg_products_viewed,
            ROUND(
                SUM(CASE WHEN conversion_status = 'converted' THEN 1 ELSE 0 END)
                / COUNT(*) * 100, 1
            ) AS conversion_rate_pct
        FROM sessions
        GROUP BY device_profile.type, device_profile.os
        ORDER BY session_count DESC
    """)
    q2_df.show(truncate=False)

    # -- Query 3: Conversion funnel analysis ----------------------------------
    print("\n--- SQL Query 3: Conversion Funnel Analysis ---")
    print("(Cross-system query: sessions from HBase + transactions from MongoDB)\n")

    q3_df = spark.sql("""
        WITH funnel AS (
            SELECT
                session_id,
                SIZE(viewed_products) > 0                       AS browsed,
                conversion_status IN ('converted', 'abandoned') AS added_to_cart,
                conversion_status = 'converted'                 AS purchased
            FROM sessions
        )
        SELECT
            COUNT(*)                                          AS total_sessions,
            SUM(CAST(browsed AS INT))                         AS browsed_products,
            ROUND(SUM(CAST(browsed AS INT)) / COUNT(*) * 100, 1)
                                                              AS browse_rate_pct,
            SUM(CAST(added_to_cart AS INT))                   AS added_to_cart,
            ROUND(SUM(CAST(added_to_cart AS INT)) / COUNT(*) * 100, 1)
                                                              AS cart_rate_pct,
            SUM(CAST(purchased AS INT))                       AS converted,
            ROUND(SUM(CAST(purchased AS INT)) / COUNT(*) * 100, 1)
                                                              AS conversion_rate_pct,
            ROUND(
                SUM(CAST(purchased AS INT))
                / NULLIF(SUM(CAST(added_to_cart AS INT)), 0) * 100, 1
            )                                                 AS cart_to_purchase_pct
        FROM funnel
    """)
    q3_df.show(truncate=False)

    # -- Query 4: Daily revenue trend with 7-day moving average ---------------
    print("\n--- SQL Query 4: Daily Revenue Trend with 7-Day Moving Average ---")
    print("(Time-series analytics combining HBase session data & MongoDB transactions)\n")

    q4_df = spark.sql("""
        WITH daily AS (
            SELECT
                DATE(timestamp)     AS sale_date,
                COUNT(*)            AS num_transactions,
                ROUND(SUM(total), 2) AS daily_revenue
            FROM transactions
            GROUP BY DATE(timestamp)
        )
        SELECT
            sale_date,
            num_transactions,
            daily_revenue,
            ROUND(
                AVG(daily_revenue) OVER (
                    ORDER BY sale_date
                    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                ), 2
            ) AS moving_avg_7d,
            ROUND(
                SUM(daily_revenue) OVER (
                    ORDER BY sale_date
                    ROWS UNBOUNDED PRECEDING
                ), 2
            ) AS cumulative_revenue
        FROM daily
        ORDER BY sale_date
    """)
    q4_df.show(100, truncate=False)

    # -- Query 5: Product price elasticity ------------------------------------
    print("\n--- SQL Query 5: Price Elasticity — Price Changes vs Sales Volume ---")
    print("(Joins product price_history with transaction volumes per product)\n")

    q5_df = spark.sql("""
        WITH price_changes AS (
            SELECT
                product_id,
                name,
                base_price,
                ph.price   AS hist_price,
                ph.date    AS price_date
            FROM products
            LATERAL VIEW explode(price_history) pht AS ph
        ),
        product_sales AS (
            SELECT
                item.product_id,
                DATE(txn.timestamp) AS sale_date,
                SUM(item.quantity) AS daily_units,
                ROUND(AVG(item.unit_price), 2) AS avg_selling_price
            FROM transactions txn
            LATERAL VIEW explode(items) itm AS item
            GROUP BY item.product_id, DATE(txn.timestamp)
        ),
        product_stats AS (
            SELECT
                ps.product_id,
                pc.name AS product_name,
                pc.base_price,
                ROUND(AVG(ps.avg_selling_price), 2) AS mean_price,
                ROUND(STDDEV(ps.avg_selling_price), 2) AS price_stddev,
                ROUND(AVG(ps.daily_units), 2)  AS mean_daily_units,
                ROUND(STDDEV(ps.daily_units), 2) AS units_stddev,
                SUM(ps.daily_units) AS total_units,
                COUNT(*) AS active_days
            FROM product_sales ps
            JOIN (SELECT DISTINCT product_id, name, base_price FROM price_changes) pc
                ON ps.product_id = pc.product_id
            GROUP BY ps.product_id, pc.name, pc.base_price
            HAVING COUNT(*) >= 3
        )
        SELECT
            product_id,
            product_name,
            base_price,
            mean_price,
            price_stddev,
            mean_daily_units,
            units_stddev,
            total_units,
            active_days,
            ROUND(
                CASE WHEN price_stddev > 0 AND mean_daily_units > 0
                     THEN (units_stddev / mean_daily_units) / (price_stddev / mean_price)
                     ELSE NULL
                END, 3
            ) AS price_variability_ratio
        FROM product_stats
        ORDER BY total_units DESC
        LIMIT 20
    """)
    q5_df.show(truncate=35)

    return {
        "top_products": q1_df.toPandas(),
        "device_behaviour": q2_df.toPandas(),
        "funnel": q3_df.toPandas(),
        "daily_revenue": q4_df.toPandas(),
        "price_elasticity": q5_df.toPandas(),
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    import sys
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    spark = (
        SparkSession.builder
        .appName("EcommerceAnalytics")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    # Reduce Spark log noise
    spark.sparkContext.setLogLevel("WARN")

    try:
        # Section 1 — Load & clean
        dfs = load_and_clean(spark)

        # Section 2 — Product recommendations
        product_recommendations(dfs)

        # Section 3 — Cohort analysis
        cohort_analysis(dfs)

        # Section 4 — Spark SQL queries
        spark_sql_queries(dfs)

        banner("ALL SECTIONS COMPLETE")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
