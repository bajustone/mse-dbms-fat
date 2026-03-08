"""
HBase sample queries for e-commerce analytics (Part 1).

Demonstrates querying the wide-column model:
  1. Retrieve all sessions for a specific user (range scan)
  2. Get recent sessions across all users (scan with limit)
  3. Product performance metrics lookup (point and range queries)

Usage: python src/hbase_queries.py
"""

import json
import os

import happybase

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
HBASE_HOST = os.environ.get("HBASE_HOST", "localhost")
HBASE_PORT = 9090
REVERSE_TS_BASE = 9999999999999


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def print_header(title: str):
    width = 72
    print()
    print("=" * width)
    print(f"  {title}")
    print("=" * width)


def decode_row(data: dict) -> dict:
    """Decode HBase row data from bytes to strings."""
    return {k.decode("utf-8"): v.decode("utf-8") for k, v in data.items()}


# ---------------------------------------------------------------------------
# Query 1: User Session History
# ---------------------------------------------------------------------------

def query_user_sessions(conn: happybase.Connection, user_id: str, limit: int = 10):
    """Retrieve sessions for a specific user using row key prefix scan.

    Row key format: {user_id}_{reverse_timestamp}
    Since reverse timestamp is used, results come back newest-first.
    """
    print_header(f"Query 1: Sessions for {user_id} (newest first, limit {limit})")

    table = conn.table("sessions")
    prefix = f"{user_id}_".encode("utf-8")

    count = 0
    for row_key, data in table.scan(row_prefix=prefix, limit=limit):
        row = decode_row(data)
        count += 1
        print(f"\n  Session #{count}: {row.get('info:session_id', 'N/A')}")
        print(f"    Time:       {row.get('info:start_time', '?')} - {row.get('info:end_time', '?')}")
        print(f"    Duration:   {row.get('info:duration_seconds', '?')}s")
        print(f"    Status:     {row.get('info:conversion_status', '?')}")
        print(f"    Referrer:   {row.get('info:referrer', '?')}")
        print(f"    Device:     {row.get('device:type', '?')} / {row.get('device:os', '?')} / {row.get('device:browser', '?')}")
        print(f"    Location:   {row.get('geo:city', '?')}, {row.get('geo:state', '?')}")

        # Parse activity data
        viewed = json.loads(row.get("activity:viewed_products", "[]"))
        cart = json.loads(row.get("activity:cart_contents", "{}"))
        print(f"    Viewed:     {len(viewed)} products")
        print(f"    Cart:       {len(cart)} items")

    if count == 0:
        print(f"  No sessions found for {user_id}.")
    else:
        print(f"\n  Total: {count} sessions returned.")


# ---------------------------------------------------------------------------
# Query 2: Recent Sessions (Global)
# ---------------------------------------------------------------------------

def query_recent_sessions(conn: happybase.Connection, limit: int = 10):
    """Scan for recent sessions across all users.

    Since row keys are sorted by user_id first, this scans alphabetically
    by user — not globally by time. For global time ordering, a secondary
    index or Spark scan would be needed.
    """
    print_header(f"Query 2: Sample Sessions (first {limit} by row key order)")

    table = conn.table("sessions")

    print(f"  {'Row Key':<40} {'Session ID':<18} {'User':<14} {'Status':<12} {'Duration':>8}")
    print(f"  {'-'*40} {'-'*18} {'-'*14} {'-'*12} {'-'*8}")

    for row_key, data in table.scan(limit=limit):
        row = decode_row(data)
        rk = row_key.decode("utf-8")
        # Extract user_id from row key
        parts = rk.rsplit("_", 1)
        user_id = parts[0] if len(parts) == 2 else rk

        print(f"  {rk:<40} {row.get('info:session_id', ''):<18} "
              f"{user_id:<14} {row.get('info:conversion_status', ''):<12} "
              f"{row.get('info:duration_seconds', ''):>8}s")


# ---------------------------------------------------------------------------
# Query 3: Product Performance Metrics
# ---------------------------------------------------------------------------

def query_product_metrics(conn: happybase.Connection, product_id: str):
    """Retrieve daily performance metrics for a specific product.

    Row key format: {product_id}_{date}
    """
    print_header(f"Query 3: Daily Metrics for {product_id}")

    table = conn.table("product_metrics")
    prefix = f"{product_id}_".encode("utf-8")

    print(f"  {'Date':<14} {'Qty':>6} {'Revenue':>12} {'Txns':>6} {'Avg Price':>10} {'Stock':>7} {'Active':>7}")
    print(f"  {'-'*14} {'-'*6} {'-'*12} {'-'*6} {'-'*10} {'-'*7} {'-'*7}")

    count = 0
    total_qty = 0
    total_rev = 0.0
    for row_key, data in table.scan(row_prefix=prefix):
        row = decode_row(data)
        count += 1
        date_str = row_key.decode("utf-8").replace(f"{product_id}_", "")
        qty = int(row.get("sales:total_quantity", "0"))
        rev = float(row.get("sales:total_revenue", "0"))
        txns = row.get("sales:num_transactions", "0")
        avg_price = row.get("sales:avg_unit_price", "0")
        stock = row.get("inventory:current_stock", "0")
        active = row.get("inventory:is_active", "?")
        total_qty += qty
        total_rev += rev

        print(f"  {date_str:<14} {qty:>6} {rev:>12,.2f} {txns:>6} {avg_price:>10} {stock:>7} {active:>7}")

    if count == 0:
        print(f"  No metrics found for {product_id}.")
    else:
        print(f"\n  Summary: {count} days, {total_qty} total units, RWF {total_rev:,.0f} total revenue")


# ---------------------------------------------------------------------------
# Query 4: Table Statistics
# ---------------------------------------------------------------------------

def query_table_stats(conn: happybase.Connection):
    """Print overall statistics for HBase tables."""
    print_header("Query 4: HBase Table Statistics")

    for table_name in ["sessions", "product_metrics"]:
        table = conn.table(table_name)
        count = sum(1 for _ in table.scan())
        families = table.families()
        family_names = [k.decode("utf-8") for k in families.keys()]
        print(f"\n  Table: {table_name}")
        print(f"    Rows:            {count:,}")
        print(f"    Column families: {', '.join(family_names)}")

        # Sample first row
        for row_key, data in table.scan(limit=1):
            cols = [k.decode("utf-8") for k in data.keys()]
            print(f"    Sample row key:  {row_key.decode('utf-8')}")
            print(f"    Columns:         {', '.join(sorted(cols))}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 72)
    print("  HBase Queries — E-Commerce Analytics (Part 1)")
    print("=" * 72)

    conn = happybase.Connection(HBASE_HOST, HBASE_PORT)

    # Verify tables exist
    tables = [t.decode() for t in conn.tables()]
    print(f"\nHBase tables: {tables}")

    required = {"sessions", "product_metrics"}
    missing = required - set(tables)
    if missing:
        print(f"\nERROR: Missing tables: {missing}")
        print("Run 'python src/seed_databases.py' first.")
        conn.close()
        return

    # Run queries
    query_table_stats(conn)
    query_user_sessions(conn, "user_000001")
    query_recent_sessions(conn, limit=15)
    query_product_metrics(conn, "prod_00001")

    conn.close()

    print("\n" + "=" * 72)
    print("  All HBase queries completed.")
    print("=" * 72)


if __name__ == "__main__":
    main()
