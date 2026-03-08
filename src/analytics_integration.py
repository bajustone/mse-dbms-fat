"""
Analytics integration: cross-system queries combining MongoDB, HBase, and Spark (Part 3).

Implements two analytical queries that benefit from data stored across different systems:
  1. Customer Lifetime Value (CLV) Estimation
  2. Funnel Conversion Analysis

Each analysis clearly documents the business question, data sources, and processing steps.
Dual-mode loading: tries HBase first, falls back to JSON files if unavailable.

Usage: python src/analytics_integration.py
"""

import json
import os
from collections import defaultdict
from datetime import datetime, timezone

import pandas as pd
from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")

MONGO_HOST = os.environ.get("MONGO_HOST", "localhost")
MONGO_PORT = 27017
MONGO_DB = "ecommerce"

HBASE_HOST = os.environ.get("HBASE_HOST", "localhost")
HBASE_PORT = 9090

REVERSE_TS_BASE = 9999999999999


# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------

def get_spark() -> SparkSession:
    """Create a local Spark session for analytics processing."""
    import sys
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    return (
        SparkSession.builder
        .appName("ECommerceAnalyticsIntegration")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Data loading helpers
# ---------------------------------------------------------------------------

def load_users_from_mongodb() -> pd.DataFrame:
    """Load user profiles with purchase summaries from MongoDB."""
    print("[MongoDB] Loading user profiles from 'users' collection...")
    client = MongoClient(MONGO_HOST, MONGO_PORT)
    db = client[MONGO_DB]

    users = []
    for doc in db.users.find():
        ps = doc.get("purchase_summary", {})
        users.append({
            "user_id": doc["_id"],
            "name": doc.get("name"),
            "city": doc.get("geo_data", {}).get("city"),
            "province": doc.get("geo_data", {}).get("province"),
            "registration_date": doc.get("registration_date"),
            "last_active": doc.get("last_active"),
            "total_orders": ps.get("total_orders", 0),
            "total_spent": ps.get("total_spent", 0.0),
            "avg_order_value": ps.get("avg_order_value", 0.0),
            "first_purchase": ps.get("first_purchase"),
            "last_purchase": ps.get("last_purchase"),
        })

    client.close()
    df = pd.DataFrame(users)
    print(f"         -> Loaded {len(df)} users from MongoDB")
    return df


def load_transactions_from_mongodb() -> pd.DataFrame:
    """Load transactions from MongoDB."""
    print("[MongoDB] Loading transactions from 'transactions' collection...")
    client = MongoClient(MONGO_HOST, MONGO_PORT)
    db = client[MONGO_DB]

    txns = []
    for doc in db.transactions.find():
        txns.append({
            "transaction_id": doc["_id"],
            "session_id": doc.get("session_id"),
            "user_id": doc.get("user_id"),
            "timestamp": doc.get("timestamp"),
            "subtotal": doc.get("subtotal", 0.0),
            "discount": doc.get("discount", 0.0),
            "total": doc.get("total", 0.0),
            "payment_method": doc.get("payment_method"),
            "status": doc.get("status"),
            "num_items": len(doc.get("items", [])),
        })

    client.close()
    df = pd.DataFrame(txns)
    print(f"         -> Loaded {len(df)} transactions from MongoDB")
    return df


def load_sessions_from_hbase() -> pd.DataFrame | None:
    """Try to load session data from HBase. Returns None if unavailable."""
    print("[HBase]   Attempting to load session data from 'sessions' table...")
    try:
        import happybase

        conn = happybase.Connection(HBASE_HOST, HBASE_PORT)
        conn.tables()  # test connection

        table = conn.table("sessions")
        sessions = []
        for row_key, data in table.scan():
            row_key_str = row_key.decode("utf-8")
            # Row key format: user_id_reverse_timestamp
            parts = row_key_str.rsplit("_", 1)
            user_id = parts[0] if len(parts) == 2 else row_key_str

            session = {
                "user_id": user_id,
                "session_id": data.get(b"info:session_id", b"").decode("utf-8"),
                "start_time": data.get(b"info:start_time", b"").decode("utf-8"),
                "end_time": data.get(b"info:end_time", b"").decode("utf-8"),
                "duration_seconds": int(data.get(b"info:duration_seconds", b"0").decode("utf-8")),
                "conversion_status": data.get(b"info:conversion_status", b"").decode("utf-8"),
                "referrer": data.get(b"info:referrer", b"").decode("utf-8"),
                "device_type": data.get(b"device:type", b"").decode("utf-8"),
                "device_os": data.get(b"device:os", b"").decode("utf-8"),
                "device_browser": data.get(b"device:browser", b"").decode("utf-8"),
                "geo_city": data.get(b"geo:city", b"").decode("utf-8"),
                "geo_state": data.get(b"geo:state", b"").decode("utf-8"),
                "cart_contents_json": data.get(b"activity:cart_contents", b"{}").decode("utf-8"),
                "viewed_products_json": data.get(b"activity:viewed_products", b"[]").decode("utf-8"),
                "page_views_json": data.get(b"activity:page_views", b"[]").decode("utf-8"),
            }
            sessions.append(session)

        conn.close()
        df = pd.DataFrame(sessions)
        print(f"          -> Loaded {len(df)} sessions from HBase")
        return df

    except Exception as e:
        print(f"          -> HBase unavailable: {e}")
        return None


def load_sessions_from_json() -> pd.DataFrame:
    """Fallback: load session data from JSON files in data/ directory."""
    print("[JSON]    Falling back to JSON files for session data...")
    all_sessions = []
    i = 0
    while True:
        path = os.path.join(DATA_DIR, f"sessions_{i}.json")
        if not os.path.exists(path):
            break
        with open(path) as f:
            all_sessions.extend(json.load(f))
        i += 1

    sessions = []
    for s in all_sessions:
        sessions.append({
            "user_id": s["user_id"],
            "session_id": s["session_id"],
            "start_time": s["start_time"],
            "end_time": s["end_time"],
            "duration_seconds": s["duration_seconds"],
            "conversion_status": s["conversion_status"],
            "referrer": s["referrer"],
            "device_type": s["device_profile"]["type"],
            "device_os": s["device_profile"]["os"],
            "device_browser": s["device_profile"]["browser"],
            "geo_city": s["geo_data"]["city"],
            "geo_state": s["geo_data"]["state"],
            "cart_contents_json": json.dumps(s["cart_contents"]),
            "viewed_products_json": json.dumps(s["viewed_products"]),
            "page_views_json": json.dumps(s["page_views"]),
        })

    df = pd.DataFrame(sessions)
    print(f"          -> Loaded {len(df)} sessions from JSON files")
    return df


def load_sessions() -> pd.DataFrame:
    """Load sessions from HBase with JSON fallback."""
    df = load_sessions_from_hbase()
    if df is not None and len(df) > 0:
        return df
    return load_sessions_from_json()


# ---------------------------------------------------------------------------
# Analysis 1: Customer Lifetime Value (CLV) Estimation
# ---------------------------------------------------------------------------

def analysis_clv(spark: SparkSession):
    """
    Customer Lifetime Value (CLV) Estimation.

    Business question:
        "What is the estimated lifetime value of each customer segment,
         and how does engagement (session frequency/duration) correlate
         with spending?"

    Data sources:
        - MongoDB `users`: purchase_summary (total_orders, total_spent, avg_order_value)
        - MongoDB `transactions`: transaction history with timestamps for recency/frequency
        - HBase `sessions`: session frequency, duration, engagement metrics per user

    Processing:
        1. Load user profiles + purchase summaries from MongoDB -> Spark DataFrame
        2. Load transaction patterns from MongoDB -> Spark DataFrame
        3. Load session engagement data from HBase (or JSON fallback) -> Spark DataFrame
        4. Join all three datasets in Spark on user_id
        5. Compute CLV = avg_order_value x purchase_frequency x estimated_lifespan
        6. Segment customers into CLV tiers (Platinum / Gold / Silver / Bronze)
        7. Correlate engagement metrics with CLV tiers
    """
    print("\n" + "=" * 70)
    print("ANALYSIS 1: Customer Lifetime Value (CLV) Estimation")
    print("=" * 70)
    print()
    print("Business question:")
    print('  "What is the estimated lifetime value of each customer segment,')
    print('   and how does engagement (session frequency/duration) correlate')
    print('   with spending?"')
    print()
    print("Data sources involved:")
    print("  - MongoDB `users`        : user profiles with purchase summaries")
    print("  - MongoDB `transactions`  : transaction history (timestamps, amounts)")
    print("  - HBase `sessions`        : browsing engagement (frequency, duration)")
    print()

    # Step 1: Load data from each source
    print("--- Step 1: Loading data from each system ---")
    users_pdf = load_users_from_mongodb()
    txns_pdf = load_transactions_from_mongodb()
    sessions_pdf = load_sessions()
    print()

    # Step 2: Convert to Spark DataFrames
    print("--- Step 2: Converting to Spark DataFrames for cross-system joins ---")
    users_sdf = spark.createDataFrame(users_pdf)
    txns_sdf = spark.createDataFrame(txns_pdf)
    sessions_sdf = spark.createDataFrame(sessions_pdf)
    print(f"  Users:        {users_sdf.count()} rows")
    print(f"  Transactions: {txns_sdf.count()} rows")
    print(f"  Sessions:     {sessions_sdf.count()} rows")
    print()

    # Step 3: Compute transaction-based metrics per user using Spark
    print("--- Step 3: Computing transaction patterns in Spark ---")
    txn_metrics = (
        txns_sdf
        .groupBy("user_id")
        .agg(
            F.count("transaction_id").alias("txn_count"),
            F.sum("total").alias("txn_total_spent"),
            F.avg("total").alias("txn_avg_value"),
            F.min("timestamp").alias("first_txn_date"),
            F.max("timestamp").alias("last_txn_date"),
        )
    )

    # Compute customer lifespan in days (from first to last transaction)
    txn_metrics = txn_metrics.withColumn(
        "lifespan_days",
        F.datediff(
            F.to_timestamp("last_txn_date", "yyyy-MM-dd'T'HH:mm:ss'Z'"),
            F.to_timestamp("first_txn_date", "yyyy-MM-dd'T'HH:mm:ss'Z'"),
        ),
    )
    # Estimate annualized purchase frequency
    txn_metrics = txn_metrics.withColumn(
        "purchase_frequency",
        F.when(F.col("lifespan_days") > 0, F.col("txn_count") / F.col("lifespan_days") * 365)
        .otherwise(F.col("txn_count")),  # single-day buyers: frequency = their order count
    )
    print("  Transaction metrics computed.")

    # Step 4: Compute session engagement metrics per user using Spark
    print("--- Step 4: Computing session engagement metrics in Spark ---")
    session_metrics = (
        sessions_sdf
        .groupBy("user_id")
        .agg(
            F.count("session_id").alias("session_count"),
            F.avg("duration_seconds").alias("avg_session_duration"),
            F.sum("duration_seconds").alias("total_session_time"),
            F.sum(
                F.when(F.col("conversion_status") == "converted", 1).otherwise(0)
            ).alias("converted_sessions"),
        )
    )
    session_metrics = session_metrics.withColumn(
        "session_conversion_rate",
        F.when(F.col("session_count") > 0, F.col("converted_sessions") / F.col("session_count"))
        .otherwise(0.0),
    )
    print("  Session engagement metrics computed.")
    print()

    # Step 5: Join all datasets in Spark (cross-system join)
    print("--- Step 5: Joining data across systems in Spark ---")
    print("  Joining: MongoDB(users) + MongoDB(transactions) + HBase/JSON(sessions)")
    clv_df = (
        users_sdf
        .select("user_id", "name", "province", "registration_date",
                "total_orders", "total_spent", "avg_order_value")
        .join(txn_metrics, on="user_id", how="left")
        .join(session_metrics, on="user_id", how="left")
    )

    # Fill nulls for users with no transactions or sessions
    clv_df = clv_df.fillna({
        "txn_count": 0,
        "txn_total_spent": 0.0,
        "txn_avg_value": 0.0,
        "lifespan_days": 0,
        "purchase_frequency": 0.0,
        "session_count": 0,
        "avg_session_duration": 0.0,
        "total_session_time": 0,
        "converted_sessions": 0,
        "session_conversion_rate": 0.0,
    })

    # Step 6: Compute CLV using simplified model
    # CLV = avg_order_value x annualized_purchase_frequency x estimated_remaining_lifespan
    # We estimate remaining lifespan as 2 years for active customers
    ESTIMATED_LIFESPAN_YEARS = 2.0

    print("--- Step 6: Computing CLV (avg_order_value x frequency x lifespan) ---")
    clv_df = clv_df.withColumn(
        "estimated_clv",
        F.col("avg_order_value") * F.col("purchase_frequency") * F.lit(ESTIMATED_LIFESPAN_YEARS),
    )

    # Step 7: Segment into CLV tiers
    print("--- Step 7: Segmenting customers into CLV tiers ---")
    clv_df = clv_df.withColumn(
        "clv_tier",
        F.when(F.col("estimated_clv") >= 5000, "Platinum")
        .when(F.col("estimated_clv") >= 2000, "Gold")
        .when(F.col("estimated_clv") >= 500, "Silver")
        .otherwise("Bronze"),
    )

    # Cache for multiple actions
    clv_df.cache()

    # --- Print Results ---
    print()
    print("=" * 70)
    print("RESULTS: CLV Estimation")
    print("=" * 70)

    # Tier summary
    print("\n--- CLV Tier Distribution ---")
    tier_summary = (
        clv_df
        .groupBy("clv_tier")
        .agg(
            F.count("user_id").alias("num_customers"),
            F.round(F.avg("estimated_clv"), 2).alias("avg_clv"),
            F.round(F.avg("total_spent"), 2).alias("avg_total_spent"),
            F.round(F.avg("session_count"), 1).alias("avg_sessions"),
            F.round(F.avg("avg_session_duration"), 1).alias("avg_session_sec"),
            F.round(F.avg("session_conversion_rate") * 100, 1).alias("avg_conv_rate_pct"),
        )
        .orderBy(F.desc("avg_clv"))
    )
    tier_summary.show(truncate=False)

    # Top 10 highest-CLV customers
    print("--- Top 10 Customers by Estimated CLV ---")
    clv_df.select(
        "user_id", "name", "province",
        F.round("estimated_clv", 2).alias("estimated_clv"),
        F.round("total_spent", 2).alias("total_spent"),
        "total_orders",
        "session_count",
        F.round("avg_session_duration", 0).alias("avg_session_sec"),
        F.round("session_conversion_rate", 3).alias("conv_rate"),
    ).orderBy(F.desc("estimated_clv")).show(10, truncate=False)

    # Engagement-CLV correlation: avg session metrics by CLV tier
    print("--- Engagement vs. CLV Correlation (by tier) ---")
    engagement_corr = (
        clv_df
        .groupBy("clv_tier")
        .agg(
            F.round(F.avg("session_count"), 1).alias("avg_sessions"),
            F.round(F.avg("total_session_time"), 0).alias("avg_total_time_sec"),
            F.round(F.avg("avg_session_duration"), 0).alias("avg_duration_sec"),
            F.round(F.avg("session_conversion_rate") * 100, 1).alias("conv_rate_pct"),
            F.round(F.avg("purchase_frequency"), 1).alias("annualized_freq"),
        )
        .orderBy(F.desc("avg_sessions"))
    )
    engagement_corr.show(truncate=False)

    # Province-level CLV summary
    print("--- Average CLV by Province ---")
    province_clv = (
        clv_df
        .groupBy("province")
        .agg(
            F.count("user_id").alias("num_customers"),
            F.round(F.avg("estimated_clv"), 2).alias("avg_clv"),
            F.round(F.sum("total_spent"), 2).alias("total_revenue"),
        )
        .orderBy(F.desc("avg_clv"))
    )
    province_clv.show(truncate=False)

    result = {
        "clv": clv_df.orderBy(F.desc("estimated_clv")).toPandas(),
        "tier_summary": tier_summary.toPandas(),
        "province_summary": province_clv.toPandas(),
    }
    clv_df.unpersist()
    print("CLV Analysis complete.")
    return result


# ---------------------------------------------------------------------------
# Analysis 2: Funnel Conversion Analysis
# ---------------------------------------------------------------------------

def analysis_funnel(spark: SparkSession):
    """
    Funnel Conversion Analysis.

    Business question:
        "What is the conversion rate at each stage of the purchase funnel,
         and which factors (device type, referrer, time of day) most
         influence conversion?"

    Data sources:
        - HBase `sessions`: browsing data, cart contents, conversion_status,
          device info, referrer (or JSON fallback)
        - MongoDB `transactions`: completed purchases linked to sessions

    Processing:
        1. Load session data from HBase (or JSON fallback) -> Spark DataFrame
        2. Load transaction data from MongoDB -> Spark DataFrame
        3. Build funnel stages in Spark: browse -> view_product -> add_to_cart -> purchase
        4. Join sessions with transactions to confirm purchases
        5. Analyze drop-off rates by device type, referrer, and time of day
    """
    print("\n" + "=" * 70)
    print("ANALYSIS 2: Funnel Conversion Analysis")
    print("=" * 70)
    print()
    print("Business question:")
    print('  "What is the conversion rate at each stage of the purchase funnel,')
    print('   and which factors (device type, referrer, time of day) most')
    print('   influence conversion?"')
    print()
    print("Data sources involved:")
    print("  - HBase `sessions`        : browsing data, cart, conversion, device, referrer")
    print("  - MongoDB `transactions`   : completed purchase records")
    print()

    # Step 1: Load data
    print("--- Step 1: Loading data from each system ---")
    sessions_pdf = load_sessions()
    txns_pdf = load_transactions_from_mongodb()
    print()

    # Step 2: Convert to Spark DataFrames
    print("--- Step 2: Converting to Spark DataFrames ---")
    sessions_sdf = spark.createDataFrame(sessions_pdf)
    txns_sdf = spark.createDataFrame(txns_pdf)
    print(f"  Sessions:     {sessions_sdf.count()} rows")
    print(f"  Transactions: {txns_sdf.count()} rows")
    print()

    # Step 3: Build funnel stages
    # Every session is a "browse". Then we determine further stages:
    #   - viewed_product: session has viewed_products (non-empty JSON array)
    #   - added_to_cart: session has non-empty cart_contents
    #   - purchased: session has conversion_status == "converted"
    print("--- Step 3: Building funnel stages in Spark ---")

    funnel_df = sessions_sdf.select(
        "session_id",
        "user_id",
        "start_time",
        "duration_seconds",
        "conversion_status",
        "referrer",
        "device_type",
        "device_os",
        "cart_contents_json",
        "viewed_products_json",
    )

    # Derive funnel stage flags
    funnel_df = funnel_df.withColumn(
        "stage_browse", F.lit(True)
    ).withColumn(
        "stage_view_product",
        # viewed_products_json != "[]"
        (F.col("viewed_products_json") != "[]") & (F.col("viewed_products_json").isNotNull()),
    ).withColumn(
        "stage_add_to_cart",
        # cart_contents_json != "{}"
        (F.col("cart_contents_json") != "{}") & (F.col("cart_contents_json").isNotNull()),
    ).withColumn(
        "stage_purchase",
        F.col("conversion_status") == "converted",
    )

    # Extract hour of day from start_time for time-of-day analysis
    funnel_df = funnel_df.withColumn(
        "hour_of_day",
        F.hour(F.to_timestamp("start_time", "yyyy-MM-dd'T'HH:mm:ss'Z'")),
    )
    # Bucket into time-of-day segments
    funnel_df = funnel_df.withColumn(
        "time_of_day",
        F.when((F.col("hour_of_day") >= 6) & (F.col("hour_of_day") < 12), "morning")
        .when((F.col("hour_of_day") >= 12) & (F.col("hour_of_day") < 18), "afternoon")
        .when((F.col("hour_of_day") >= 18) & (F.col("hour_of_day") < 22), "evening")
        .otherwise("night"),
    )

    funnel_df.cache()

    # Step 4: Join with transactions to enrich purchase data
    print("--- Step 4: Joining sessions (HBase/JSON) with transactions (MongoDB) ---")
    # Get transaction amounts per session
    txn_by_session = (
        txns_sdf
        .groupBy("session_id")
        .agg(
            F.sum("total").alias("purchase_amount"),
            F.sum("num_items").alias("items_purchased"),
        )
    )
    funnel_enriched = funnel_df.join(txn_by_session, on="session_id", how="left")
    funnel_enriched = funnel_enriched.fillna({"purchase_amount": 0.0, "items_purchased": 0})
    print("  Sessions joined with transaction amounts.")
    print()

    # --- Print Results ---
    print("=" * 70)
    print("RESULTS: Funnel Conversion Analysis")
    print("=" * 70)

    # Overall funnel
    total = funnel_df.count()
    browse_count = funnel_df.filter(F.col("stage_browse")).count()
    view_count = funnel_df.filter(F.col("stage_view_product")).count()
    cart_count = funnel_df.filter(F.col("stage_add_to_cart")).count()
    purchase_count = funnel_df.filter(F.col("stage_purchase")).count()

    print("\n--- Overall Funnel ---")
    print(f"  {'Stage':<20} {'Count':>8} {'% of Total':>12} {'% of Prev':>12}")
    print(f"  {'-'*20} {'-'*8} {'-'*12} {'-'*12}")
    stages = [
        ("Browse", browse_count, browse_count),
        ("View Product", view_count, browse_count),
        ("Add to Cart", cart_count, view_count),
        ("Purchase", purchase_count, cart_count),
    ]
    for name, count, prev in stages:
        pct_total = (count / total * 100) if total > 0 else 0
        pct_prev = (count / prev * 100) if prev > 0 else 0
        print(f"  {name:<20} {count:>8} {pct_total:>11.1f}% {pct_prev:>11.1f}%")
    print()

    # Funnel by device type
    print("--- Funnel Conversion by Device Type ---")
    device_funnel = (
        funnel_df
        .groupBy("device_type")
        .agg(
            F.count("session_id").alias("total_sessions"),
            F.sum(F.col("stage_view_product").cast("int")).alias("viewed"),
            F.sum(F.col("stage_add_to_cart").cast("int")).alias("added_to_cart"),
            F.sum(F.col("stage_purchase").cast("int")).alias("purchased"),
        )
    )
    device_funnel = device_funnel.withColumn(
        "view_rate_pct", F.round(F.col("viewed") / F.col("total_sessions") * 100, 1),
    ).withColumn(
        "cart_rate_pct", F.round(F.col("added_to_cart") / F.col("total_sessions") * 100, 1),
    ).withColumn(
        "purchase_rate_pct", F.round(F.col("purchased") / F.col("total_sessions") * 100, 1),
    ).orderBy(F.desc("purchase_rate_pct"))
    device_funnel.show(truncate=False)

    # Funnel by referrer
    print("--- Funnel Conversion by Referrer ---")
    referrer_funnel = (
        funnel_df
        .groupBy("referrer")
        .agg(
            F.count("session_id").alias("total_sessions"),
            F.sum(F.col("stage_view_product").cast("int")).alias("viewed"),
            F.sum(F.col("stage_add_to_cart").cast("int")).alias("added_to_cart"),
            F.sum(F.col("stage_purchase").cast("int")).alias("purchased"),
        )
    )
    referrer_funnel = referrer_funnel.withColumn(
        "view_rate_pct", F.round(F.col("viewed") / F.col("total_sessions") * 100, 1),
    ).withColumn(
        "cart_rate_pct", F.round(F.col("added_to_cart") / F.col("total_sessions") * 100, 1),
    ).withColumn(
        "purchase_rate_pct", F.round(F.col("purchased") / F.col("total_sessions") * 100, 1),
    ).orderBy(F.desc("purchase_rate_pct"))
    referrer_funnel.show(truncate=False)

    # Funnel by time of day
    print("--- Funnel Conversion by Time of Day ---")
    tod_funnel = (
        funnel_df
        .groupBy("time_of_day")
        .agg(
            F.count("session_id").alias("total_sessions"),
            F.sum(F.col("stage_view_product").cast("int")).alias("viewed"),
            F.sum(F.col("stage_add_to_cart").cast("int")).alias("added_to_cart"),
            F.sum(F.col("stage_purchase").cast("int")).alias("purchased"),
        )
    )
    tod_funnel = tod_funnel.withColumn(
        "view_rate_pct", F.round(F.col("viewed") / F.col("total_sessions") * 100, 1),
    ).withColumn(
        "cart_rate_pct", F.round(F.col("added_to_cart") / F.col("total_sessions") * 100, 1),
    ).withColumn(
        "purchase_rate_pct", F.round(F.col("purchased") / F.col("total_sessions") * 100, 1),
    ).orderBy(F.desc("purchase_rate_pct"))
    tod_funnel.show(truncate=False)

    # Average purchase amount by device and referrer (for converted sessions)
    print("--- Avg Purchase Amount for Converted Sessions ---")
    purchase_avg = (
        funnel_enriched
        .filter(F.col("stage_purchase"))
        .groupBy("device_type", "referrer")
        .agg(
            F.count("session_id").alias("purchases"),
            F.round(F.avg("purchase_amount"), 2).alias("avg_amount"),
            F.round(F.avg("items_purchased"), 1).alias("avg_items"),
        )
        .orderBy(F.desc("avg_amount"))
    )
    purchase_avg.show(20, truncate=False)

    # Cart abandonment analysis
    print("--- Cart Abandonment Analysis ---")
    cart_sessions = funnel_df.filter(F.col("stage_add_to_cart"))
    abandoned = cart_sessions.filter(F.col("conversion_status") == "abandoned")
    converted = cart_sessions.filter(F.col("conversion_status") == "converted")
    total_cart = cart_sessions.count()
    abandoned_count = abandoned.count()
    converted_count = converted.count()
    abandonment_rate = (abandoned_count / total_cart * 100) if total_cart > 0 else 0

    print(f"  Sessions with items in cart: {total_cart}")
    print(f"  Abandoned:                   {abandoned_count} ({abandonment_rate:.1f}%)")
    print(f"  Converted:                   {converted_count} ({100 - abandonment_rate:.1f}%)")
    print()

    # Abandonment by device
    print("  Cart abandonment by device type:")
    abandon_by_device = (
        cart_sessions
        .groupBy("device_type")
        .agg(
            F.count("session_id").alias("cart_sessions"),
            F.sum(F.when(F.col("conversion_status") == "abandoned", 1).otherwise(0)).alias("abandoned"),
        )
    )
    abandon_by_device = abandon_by_device.withColumn(
        "abandonment_rate_pct",
        F.round(F.col("abandoned") / F.col("cart_sessions") * 100, 1),
    ).orderBy(F.desc("abandonment_rate_pct"))
    abandon_by_device.show(truncate=False)

    result = {
        "funnel_device": device_funnel.toPandas(),
        "funnel_referrer": referrer_funnel.toPandas(),
        "cart_abandonment": abandon_by_device.toPandas(),
    }
    funnel_df.unpersist()
    print("Funnel Conversion Analysis complete.")
    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 70)
    print("Part 3: Analytics Integration — Cross-System Queries")
    print("=" * 70)
    print()
    print("This module demonstrates analytical queries that benefit from data")
    print("stored across multiple database systems (MongoDB, HBase) and uses")
    print("Apache Spark for cross-system joins and complex computation.")
    print()

    spark = get_spark()

    try:
        # Analysis 1: Customer Lifetime Value
        analysis_clv(spark)

        # Analysis 2: Funnel Conversion Analysis
        analysis_funnel(spark)
    finally:
        spark.stop()

    print()
    print("=" * 70)
    print("Analytics Integration complete.")
    print("=" * 70)


if __name__ == "__main__":
    main()
