"""
Seed MongoDB and HBase with generated e-commerce data.

- MongoDB: products (denormalized with categories), users (with purchase summaries), transactions
- HBase: sessions (time-series), product_metrics (daily aggregates)

Usage: python src/seed_databases.py
"""

import json
import os
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone

import happybase
from pymongo import ASCENDING, MongoClient

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")

MONGO_HOST = os.environ.get("MONGO_HOST", "localhost")
MONGO_PORT = 27017
MONGO_DB = "ecommerce"

HBASE_HOST = os.environ.get("HBASE_HOST", "localhost")
HBASE_PORT = 9090

HBASE_BATCH_SIZE = 500
REVERSE_TS_BASE = 9999999999999


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_json(filename: str) -> list[dict]:
    path = os.path.join(DATA_DIR, filename)
    with open(path) as f:
        return json.load(f)


def load_all_data() -> dict:
    """Load all JSON files from data/."""
    print("Loading data files...")
    categories = load_json("categories.json")
    products = load_json("products.json")
    users = load_json("users.json")
    transactions = load_json("transactions.json")

    # Sessions are split across multiple files
    sessions = []
    i = 0
    while True:
        path = os.path.join(DATA_DIR, f"sessions_{i}.json")
        if not os.path.exists(path):
            break
        sessions.extend(load_json(f"sessions_{i}.json"))
        i += 1

    print(f"  Loaded: {len(categories)} categories, {len(products)} products, "
          f"{len(users)} users, {len(sessions)} sessions, {len(transactions)} transactions")
    return {
        "categories": categories,
        "products": products,
        "users": users,
        "sessions": sessions,
        "transactions": transactions,
    }


# ---------------------------------------------------------------------------
# Lookup builders
# ---------------------------------------------------------------------------

def build_category_map(categories: list[dict]) -> dict:
    """Build lookup: (category_id, subcategory_id) -> category/subcategory info."""
    cat_map = {}
    for cat in categories:
        for sub in cat["subcategories"]:
            cat_map[(cat["category_id"], sub["subcategory_id"])] = {
                "category_id": cat["category_id"],
                "name": cat["name"],
                "subcategory_id": sub["subcategory_id"],
                "subcategory_name": sub["name"],
                "profit_margin": sub["profit_margin"],
            }
    return cat_map


def build_purchase_summaries(transactions: list[dict]) -> dict:
    """Compute per-user purchase summary from transactions."""
    user_txns: dict[str, list[dict]] = defaultdict(list)
    for txn in transactions:
        user_txns[txn["user_id"]].append(txn)

    summaries = {}
    for user_id, txns in user_txns.items():
        total_spent = sum(t["total"] for t in txns)
        timestamps = [t["timestamp"] for t in txns]
        payment_methods = [t["payment_method"] for t in txns]
        most_common_payment = Counter(payment_methods).most_common(1)[0][0]

        summaries[user_id] = {
            "total_orders": len(txns),
            "total_spent": round(total_spent, 2),
            "avg_order_value": round(total_spent / len(txns), 2),
            "first_purchase": min(timestamps),
            "last_purchase": max(timestamps),
            "favorite_payment_method": most_common_payment,
        }
    return summaries


def build_product_daily_metrics(
    transactions: list[dict], products: list[dict],
) -> dict:
    """Aggregate transaction items into daily product metrics."""
    # product lookup for inventory info
    prod_lookup = {p["product_id"]: p for p in products}

    # Accumulate: (product_id, date) -> {quantity, revenue, num_txns, prices}
    metrics: dict[tuple[str, str], dict] = defaultdict(
        lambda: {"total_quantity": 0, "total_revenue": 0.0, "num_transactions": 0, "prices": []}
    )

    for txn in transactions:
        txn_date = txn["timestamp"][:10]  # "YYYY-MM-DD"
        for item in txn["items"]:
            key = (item["product_id"], txn_date)
            metrics[key]["total_quantity"] += item["quantity"]
            metrics[key]["total_revenue"] += item["subtotal"]
            metrics[key]["num_transactions"] += 1
            metrics[key]["prices"].append(item["unit_price"])

    # Finalize: compute avg_unit_price and attach inventory info
    result = {}
    for (product_id, date), m in metrics.items():
        avg_price = round(sum(m["prices"]) / len(m["prices"]), 2)
        prod = prod_lookup.get(product_id, {})
        result[(product_id, date)] = {
            "total_quantity": m["total_quantity"],
            "total_revenue": round(m["total_revenue"], 2),
            "num_transactions": m["num_transactions"],
            "avg_unit_price": avg_price,
            "current_stock": prod.get("current_stock", 0),
            "base_price": prod.get("base_price", 0.0),
            "is_active": prod.get("is_active", False),
        }
    return result


# ---------------------------------------------------------------------------
# MongoDB seeding
# ---------------------------------------------------------------------------

def seed_mongodb(data: dict):
    print("\n--- Seeding MongoDB ---")
    client = MongoClient(MONGO_HOST, MONGO_PORT)
    db = client[MONGO_DB]

    cat_map = build_category_map(data["categories"])
    purchase_summaries = build_purchase_summaries(data["transactions"])

    # -- Products --
    print("  Inserting products...")
    db.products.drop()
    product_docs = []
    for p in data["products"]:
        cat_info = cat_map.get((p["category_id"], p["subcategory_id"]), {})
        doc = {
            "_id": p["product_id"],
            "name": p["name"],
            "category": cat_info,
            "base_price": p["base_price"],
            "current_stock": p["current_stock"],
            "is_active": p["is_active"],
            "price_history": p["price_history"],
            "creation_date": p["creation_date"],
        }
        product_docs.append(doc)
    db.products.insert_many(product_docs)
    db.products.create_index([("category.category_id", ASCENDING)])
    db.products.create_index([("base_price", ASCENDING)])
    db.products.create_index([("is_active", ASCENDING)])
    print(f"    -> {db.products.count_documents({})} products inserted")

    # -- Users --
    print("  Inserting users...")
    db.users.drop()
    user_docs = []
    empty_summary = {
        "total_orders": 0,
        "total_spent": 0.0,
        "avg_order_value": 0.0,
        "first_purchase": None,
        "last_purchase": None,
        "favorite_payment_method": None,
    }
    for u in data["users"]:
        summary = purchase_summaries.get(u["user_id"], empty_summary)
        doc = {
            "_id": u["user_id"],
            "name": u["name"],
            "email": u["email"],
            "phone": u["phone"],
            "geo_data": u["geo_data"],
            "registration_date": u["registration_date"],
            "last_active": u["last_active"],
            "purchase_summary": summary,
        }
        user_docs.append(doc)
    db.users.insert_many(user_docs)
    db.users.create_index([("geo_data.province", ASCENDING)])
    db.users.create_index([("purchase_summary.total_spent", ASCENDING)])
    db.users.create_index([("registration_date", ASCENDING)])
    print(f"    -> {db.users.count_documents({})} users inserted")

    # -- Transactions --
    print("  Inserting transactions...")
    db.transactions.drop()
    txn_docs = []
    for t in data["transactions"]:
        doc = {**t, "_id": t["transaction_id"]}
        del doc["transaction_id"]
        txn_docs.append(doc)
    db.transactions.insert_many(txn_docs)
    db.transactions.create_index([("user_id", ASCENDING)])
    db.transactions.create_index([("timestamp", ASCENDING)])
    db.transactions.create_index([("status", ASCENDING)])
    db.transactions.create_index([("items.product_id", ASCENDING)])
    print(f"    -> {db.transactions.count_documents({})} transactions inserted")

    client.close()
    print("  MongoDB seeding complete.")


# ---------------------------------------------------------------------------
# HBase seeding
# ---------------------------------------------------------------------------

def _ensure_table(conn: happybase.Connection, table_name: str, families: dict):
    """Drop and recreate an HBase table."""
    tables = [t.decode() for t in conn.tables()]
    if table_name in tables:
        conn.disable_table(table_name)
        conn.delete_table(table_name)
    conn.create_table(table_name, families)


def _to_bytes(value) -> bytes:
    """Encode a value as bytes for HBase storage."""
    if isinstance(value, bytes):
        return value
    return str(value).encode("utf-8")


def _connect_hbase(max_retries: int = 40, delay: int = 15) -> happybase.Connection:
    """Connect to HBase with retries (it can take minutes to start under emulation).

    Verifies the master is fully initialized by attempting a test createTable/
    deleteTable cycle, since conn.tables() can succeed before the master is
    ready to handle DDL operations (PleaseHoldException).
    """
    test_table = "__hbase_ready_check__"
    for attempt in range(1, max_retries + 1):
        try:
            conn = happybase.Connection(HBASE_HOST, HBASE_PORT)
            conn.tables()  # basic Thrift connectivity check
            # Verify master is fully initialized by attempting DDL
            existing = [t.decode() for t in conn.tables()]
            if test_table in existing:
                conn.disable_table(test_table)
                conn.delete_table(test_table)
            conn.create_table(test_table, {"t": {}})
            conn.disable_table(test_table)
            conn.delete_table(test_table)
            print(f"    HBase ready (attempt {attempt}).")
            return conn
        except Exception as e:
            if attempt == max_retries:
                raise
            msg = str(e)
            # Shorten verbose Java stack traces for display
            if "Master is initializing" in msg:
                msg = "Master is initializing"
            print(f"    HBase not ready (attempt {attempt}/{max_retries}): {msg}")
            print(f"    Retrying in {delay}s...")
            time.sleep(delay)
    raise RuntimeError("Failed to connect to HBase")


def seed_hbase(data: dict):
    print("\n--- Seeding HBase ---")
    print("  Connecting to HBase (may take a few minutes if still starting)...")
    conn = _connect_hbase()

    # -- Sessions table --
    print("  Creating sessions table...")
    _ensure_table(conn, "sessions", {
        "info": {},
        "device": {},
        "geo": {},
        "activity": {},
    })

    table = conn.table("sessions")
    print("  Inserting sessions...")
    batch = table.batch(batch_size=HBASE_BATCH_SIZE)
    for s in data["sessions"]:
        # Row key: user_id + reverse timestamp
        start_dt = datetime.strptime(s["start_time"], "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc
        )
        epoch_ms = int(start_dt.timestamp() * 1000)
        reverse_ts = REVERSE_TS_BASE - epoch_ms

        row_key = f"{s['user_id']}_{reverse_ts}"

        row_data = {
            # info family
            b"info:session_id": _to_bytes(s["session_id"]),
            b"info:start_time": _to_bytes(s["start_time"]),
            b"info:end_time": _to_bytes(s["end_time"]),
            b"info:duration_seconds": _to_bytes(s["duration_seconds"]),
            b"info:conversion_status": _to_bytes(s["conversion_status"]),
            b"info:referrer": _to_bytes(s["referrer"]),
            # device family
            b"device:type": _to_bytes(s["device_profile"]["type"]),
            b"device:os": _to_bytes(s["device_profile"]["os"]),
            b"device:browser": _to_bytes(s["device_profile"]["browser"]),
            # geo family
            b"geo:city": _to_bytes(s["geo_data"]["city"]),
            b"geo:state": _to_bytes(s["geo_data"]["state"]),
            b"geo:country": _to_bytes(s["geo_data"]["country"]),
            b"geo:ip_address": _to_bytes(s["geo_data"]["ip_address"]),
            # activity family (JSON-serialized)
            b"activity:viewed_products": _to_bytes(json.dumps(s["viewed_products"])),
            b"activity:page_views": _to_bytes(json.dumps(s["page_views"])),
            b"activity:cart_contents": _to_bytes(json.dumps(s["cart_contents"])),
        }
        batch.put(row_key.encode("utf-8"), row_data)

    batch.send()
    session_count = sum(1 for _ in table.scan(columns=[b"info:session_id"]))
    print(f"    -> {session_count} sessions inserted")

    # -- Product metrics table --
    print("  Creating product_metrics table...")
    _ensure_table(conn, "product_metrics", {
        "sales": {},
        "inventory": {},
    })

    metrics = build_product_daily_metrics(data["transactions"], data["products"])
    table2 = conn.table("product_metrics")
    print("  Inserting product metrics...")
    batch2 = table2.batch(batch_size=HBASE_BATCH_SIZE)
    for (product_id, date), m in metrics.items():
        row_key = f"{product_id}_{date}"
        row_data = {
            b"sales:total_quantity": _to_bytes(m["total_quantity"]),
            b"sales:total_revenue": _to_bytes(m["total_revenue"]),
            b"sales:num_transactions": _to_bytes(m["num_transactions"]),
            b"sales:avg_unit_price": _to_bytes(m["avg_unit_price"]),
            b"inventory:current_stock": _to_bytes(m["current_stock"]),
            b"inventory:base_price": _to_bytes(m["base_price"]),
            b"inventory:is_active": _to_bytes(m["is_active"]),
        }
        batch2.put(row_key.encode("utf-8"), row_data)

    batch2.send()
    metrics_count = sum(1 for _ in table2.scan(columns=[b"sales:total_quantity"]))
    print(f"    -> {metrics_count} product-day metric rows inserted")

    conn.close()
    print("  HBase seeding complete.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 60)
    print("E-Commerce Database Seeder")
    print("=" * 60)

    data = load_all_data()
    seed_mongodb(data)
    seed_hbase(data)

    print("\n" + "=" * 60)
    print("Seeding complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
