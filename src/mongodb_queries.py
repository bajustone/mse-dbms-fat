"""
MongoDB aggregation queries for e-commerce analytics (Part 1).

Demonstrates non-trivial aggregation pipelines on the document model:
  1. Product Popularity Analysis — top-selling products by quantity
  2. Revenue Analytics by Category — revenue per category with subcategory breakdown
  3. User Segmentation by Purchase Behavior — spending tiers and geographic distribution
  4. Monthly Revenue Trend — revenue, transaction count, and discount rates over time
  5. Payment Method Analysis — revenue and discount patterns by payment method

Usage: python src/mongodb_queries.py
"""

import os

from pymongo import MongoClient

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
MONGO_HOST = os.environ.get("MONGO_HOST", "localhost")
MONGO_PORT = 27017
MONGO_DB = "ecommerce"


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def print_header(title: str):
    """Print a section header."""
    width = 72
    print()
    print("=" * width)
    print(f"  {title}")
    print("=" * width)


def print_table(rows: list[dict], columns: list[tuple[str, str, int]]):
    """Print a list of dicts as a formatted table.

    columns: list of (dict_key, display_header, width)
    """
    header_parts = []
    sep_parts = []
    for _, display, width in columns:
        header_parts.append(f"{display:<{width}}")
        sep_parts.append("-" * width)
    print("  " + "  ".join(header_parts))
    print("  " + "  ".join(sep_parts))

    for row in rows:
        parts = []
        for key, _, width in columns:
            val = row.get(key, "")
            if isinstance(val, float):
                val = f"{val:,.2f}"
            elif isinstance(val, int):
                val = f"{val:,}"
            parts.append(f"{str(val):<{width}}")
        print("  " + "  ".join(parts))


# ---------------------------------------------------------------------------
# Query 1: Product Popularity Analysis
# ---------------------------------------------------------------------------

def query_product_popularity(db):
    """Top 10 best-selling products by total quantity sold.

    Pipeline:
      - $match only completed transactions
      - $unwind the items array to get one doc per line item
      - $group by product_id, summing quantity and revenue
      - $sort descending by total quantity
      - $limit to top 10
      - $lookup product details from the products collection
      - $unwind the joined product doc
      - $project the final shape
    """
    print_header("Query 1: Top 10 Best-Selling Products")

    pipeline = [
        {"$match": {"status": "completed"}},
        {"$unwind": "$items"},
        {
            "$group": {
                "_id": "$items.product_id",
                "total_qty": {"$sum": "$items.quantity"},
                "total_revenue": {"$sum": "$items.subtotal"},
                "num_orders": {"$sum": 1},
            }
        },
        {"$sort": {"total_qty": -1}},
        {"$limit": 10},
        {
            "$lookup": {
                "from": "products",
                "localField": "_id",
                "foreignField": "_id",
                "as": "product",
            }
        },
        {"$unwind": "$product"},
        {
            "$project": {
                "_id": 0,
                "product_id": "$_id",
                "name": "$product.name",
                "category": "$product.category.name",
                "total_qty": 1,
                "total_revenue": {"$round": ["$total_revenue", 2]},
                "num_orders": 1,
            }
        },
    ]

    results = list(db.transactions.aggregate(pipeline))
    print_table(results, [
        ("product_id", "Product ID", 12),
        ("name", "Name", 28),
        ("category", "Category", 16),
        ("total_qty", "Qty Sold", 10),
        ("total_revenue", "Revenue", 12),
        ("num_orders", "Orders", 8),
    ])
    return results


# ---------------------------------------------------------------------------
# Query 2: Revenue Analytics by Category
# ---------------------------------------------------------------------------

def query_revenue_by_category(db):
    """Total revenue per category with subcategory breakdown.

    Pipeline:
      - $match completed transactions
      - $unwind items
      - $lookup products to get category info
      - $unwind joined product
      - $group by category and subcategory
      - $sort by revenue descending
      - $group again by category, pushing subcategory details into an array
      - $project final shape with category totals
    """
    print_header("Query 2: Revenue by Category (with Subcategory Breakdown)")

    pipeline = [
        {"$match": {"status": "completed"}},
        {"$unwind": "$items"},
        {
            "$lookup": {
                "from": "products",
                "localField": "items.product_id",
                "foreignField": "_id",
                "as": "product",
            }
        },
        {"$unwind": "$product"},
        {
            "$group": {
                "_id": {
                    "category": "$product.category.name",
                    "subcategory": "$product.category.subcategory_name",
                },
                "revenue": {"$sum": "$items.subtotal"},
                "qty_sold": {"$sum": "$items.quantity"},
                "order_count": {"$sum": 1},
            }
        },
        {"$sort": {"revenue": -1}},
        {
            "$group": {
                "_id": "$_id.category",
                "total_revenue": {"$sum": "$revenue"},
                "total_qty": {"$sum": "$qty_sold"},
                "total_orders": {"$sum": "$order_count"},
                "subcategories": {
                    "$push": {
                        "name": "$_id.subcategory",
                        "revenue": {"$round": ["$revenue", 2]},
                        "qty_sold": "$qty_sold",
                    }
                },
            }
        },
        {"$sort": {"total_revenue": -1}},
        {
            "$project": {
                "_id": 0,
                "category": "$_id",
                "total_revenue": {"$round": ["$total_revenue", 2]},
                "total_qty": 1,
                "total_orders": 1,
                "avg_order_value": {
                    "$round": [{"$divide": ["$total_revenue", "$total_orders"]}, 2]
                },
                "subcategories": 1,
            }
        },
    ]

    results = list(db.transactions.aggregate(pipeline))

    # Print category-level summary
    print_table(results, [
        ("category", "Category", 22),
        ("total_revenue", "Revenue", 14),
        ("total_qty", "Qty Sold", 10),
        ("total_orders", "Orders", 8),
        ("avg_order_value", "Avg Order", 12),
    ])

    # Print subcategory breakdown for top 3 categories
    print("\n  Subcategory breakdown (top 3 categories):")
    for cat in results[:3]:
        print(f"\n    {cat['category']}:")
        for sub in sorted(cat["subcategories"], key=lambda s: s["revenue"], reverse=True):
            print(f"      - {sub['name']:<26} Revenue: {sub['revenue']:>10,.2f}   Qty: {sub['qty_sold']:>5,}")

    return results


# ---------------------------------------------------------------------------
# Query 3: User Segmentation by Purchase Behavior
# ---------------------------------------------------------------------------

def query_user_segmentation(db):
    """Segment users into spending tiers and show geographic distribution.

    Pipeline:
      - $bucket users by purchase_summary.total_spent into tiers
      - Show count, avg spending, and representative provinces per tier

    Second pipeline:
      - $group by province, computing average spend and user count
      - $sort by average spend descending
    """
    print_header("Query 3: User Segmentation by Spending Tier")

    # 3a: Bucket users into spending tiers
    bucket_pipeline = [
        {
            "$bucket": {
                "groupBy": "$purchase_summary.total_spent",
                "boundaries": [0, 1, 5000, 20000, 50000, 1000000],
                "default": "Other",
                "output": {
                    "count": {"$sum": 1},
                    "avg_spent": {"$avg": "$purchase_summary.total_spent"},
                    "total_spent": {"$sum": "$purchase_summary.total_spent"},
                    "avg_orders": {"$avg": "$purchase_summary.total_orders"},
                },
            }
        },
    ]

    tier_labels = {0: "Inactive (0)", 1: "Low (<5K)", 5000: "Regular (5K-20K)",
                   20000: "High (20K-50K)", 50000: "VIP (50K+)", "Other": "Other"}

    results_bucket = list(db.users.aggregate(bucket_pipeline))
    for r in results_bucket:
        r["tier"] = tier_labels.get(r["_id"], str(r["_id"]))
        r["avg_spent"] = round(r["avg_spent"], 2)
        r["total_spent"] = round(r["total_spent"], 2)
        r["avg_orders"] = round(r["avg_orders"], 1)

    print_table(results_bucket, [
        ("tier", "Tier", 20),
        ("count", "Users", 8),
        ("avg_spent", "Avg Spent", 12),
        ("total_spent", "Total Spent", 14),
        ("avg_orders", "Avg Orders", 12),
    ])

    # 3b: Spending by province
    print_header("Query 3b: Average Spending by Province")

    province_pipeline = [
        {"$match": {"purchase_summary.total_orders": {"$gt": 0}}},
        {
            "$group": {
                "_id": "$geo_data.province",
                "user_count": {"$sum": 1},
                "avg_spent": {"$avg": "$purchase_summary.total_spent"},
                "total_spent": {"$sum": "$purchase_summary.total_spent"},
                "avg_orders": {"$avg": "$purchase_summary.total_orders"},
            }
        },
        {"$sort": {"avg_spent": -1}},
        {
            "$project": {
                "_id": 0,
                "province": "$_id",
                "user_count": 1,
                "avg_spent": {"$round": ["$avg_spent", 2]},
                "total_spent": {"$round": ["$total_spent", 2]},
                "avg_orders": {"$round": ["$avg_orders", 1]},
            }
        },
    ]

    results_province = list(db.users.aggregate(province_pipeline))
    print_table(results_province, [
        ("province", "Province", 16),
        ("user_count", "Users", 8),
        ("avg_spent", "Avg Spent", 12),
        ("total_spent", "Total Spent", 14),
        ("avg_orders", "Avg Orders", 12),
    ])

    return results_bucket, results_province


# ---------------------------------------------------------------------------
# Query 4: Monthly Revenue Trend
# ---------------------------------------------------------------------------

def query_monthly_revenue_trend(db):
    """Monthly revenue, transaction count, and discount analysis.

    Pipeline:
      - $match completed transactions
      - $addFields to extract year-month from timestamp string
      - $group by year-month
      - $sort chronologically
      - $project final shape with discount rate
    """
    print_header("Query 4: Monthly Revenue Trend")

    pipeline = [
        {"$match": {"status": "completed"}},
        {
            "$addFields": {
                "year_month": {"$substr": ["$timestamp", 0, 7]},
            }
        },
        {
            "$group": {
                "_id": "$year_month",
                "total_revenue": {"$sum": "$total"},
                "total_subtotal": {"$sum": "$subtotal"},
                "total_discount": {"$sum": "$discount"},
                "txn_count": {"$sum": 1},
                "avg_order_value": {"$avg": "$total"},
            }
        },
        {"$sort": {"_id": 1}},
        {
            "$project": {
                "_id": 0,
                "month": "$_id",
                "total_revenue": {"$round": ["$total_revenue", 2]},
                "txn_count": 1,
                "avg_order_value": {"$round": ["$avg_order_value", 2]},
                "total_discount": {"$round": ["$total_discount", 2]},
                "discount_rate": {
                    "$round": [
                        {
                            "$multiply": [
                                {"$divide": ["$total_discount", "$total_subtotal"]},
                                100,
                            ]
                        },
                        2,
                    ]
                },
            }
        },
    ]

    results = list(db.transactions.aggregate(pipeline))
    print_table(results, [
        ("month", "Month", 10),
        ("total_revenue", "Revenue", 14),
        ("txn_count", "Transactions", 14),
        ("avg_order_value", "Avg Order", 12),
        ("total_discount", "Discounts", 12),
        ("discount_rate", "Disc %", 8),
    ])
    return results


# ---------------------------------------------------------------------------
# Query 5: Payment Method Analysis
# ---------------------------------------------------------------------------

def query_payment_method_analysis(db):
    """Revenue and discount patterns by payment method.

    Pipeline:
      - $match completed transactions
      - $group by payment_method
      - $sort by revenue descending
      - $project with computed discount rate and avg order value
    """
    print_header("Query 5: Payment Method Analysis")

    pipeline = [
        {"$match": {"status": "completed"}},
        {
            "$group": {
                "_id": "$payment_method",
                "total_revenue": {"$sum": "$total"},
                "total_subtotal": {"$sum": "$subtotal"},
                "total_discount": {"$sum": "$discount"},
                "txn_count": {"$sum": 1},
                "avg_order_value": {"$avg": "$total"},
                "unique_users": {"$addToSet": "$user_id"},
            }
        },
        {"$sort": {"total_revenue": -1}},
        {
            "$project": {
                "_id": 0,
                "payment_method": "$_id",
                "total_revenue": {"$round": ["$total_revenue", 2]},
                "txn_count": 1,
                "avg_order_value": {"$round": ["$avg_order_value", 2]},
                "total_discount": {"$round": ["$total_discount", 2]},
                "discount_rate": {
                    "$round": [
                        {
                            "$multiply": [
                                {"$divide": ["$total_discount", "$total_subtotal"]},
                                100,
                            ]
                        },
                        2,
                    ]
                },
                "unique_users": {"$size": "$unique_users"},
            }
        },
    ]

    results = list(db.transactions.aggregate(pipeline))
    print_table(results, [
        ("payment_method", "Payment Method", 20),
        ("total_revenue", "Revenue", 14),
        ("txn_count", "Transactions", 14),
        ("avg_order_value", "Avg Order", 12),
        ("total_discount", "Discounts", 12),
        ("discount_rate", "Disc %", 8),
        ("unique_users", "Users", 8),
    ])
    return results


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 72)
    print("  MongoDB Aggregation Queries — E-Commerce Analytics (Part 1)")
    print("=" * 72)

    client = MongoClient(MONGO_HOST, MONGO_PORT)
    db = client[MONGO_DB]

    # Verify collections exist
    colls = db.list_collection_names()
    required = {"products", "users", "transactions"}
    missing = required - set(colls)
    if missing:
        print(f"\nERROR: Missing collections: {missing}")
        print("Run 'python src/seed_databases.py' first.")
        client.close()
        return

    print(f"\nConnected to MongoDB '{MONGO_DB}' database.")
    print(f"Collections: {', '.join(sorted(colls))}")
    print(f"  products:     {db.products.count_documents({}):,} docs")
    print(f"  users:        {db.users.count_documents({}):,} docs")
    print(f"  transactions: {db.transactions.count_documents({}):,} docs")

    query_product_popularity(db)
    query_revenue_by_category(db)
    query_user_segmentation(db)
    query_monthly_revenue_trend(db)
    query_payment_method_analysis(db)

    client.close()

    print("\n" + "=" * 72)
    print("  All queries completed.")
    print("=" * 72)


if __name__ == "__main__":
    main()
