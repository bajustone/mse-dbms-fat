"""
Visualizations and business insights for e-commerce analytics (Part 4).

Generates 4 publication-quality charts saved to the output/ directory:
  1. Revenue Over Time by Category (multi-line chart)
  2. Customer Segmentation (scatter + bar breakdown)
  3. Product Performance Dashboard (horizontal bar chart)
  4. Conversion Funnel (funnel bar chart by device type)

Data is loaded from MongoDB (localhost:27017, db 'ecommerce') when available,
falling back to JSON files in the data/ directory.

Usage: python src/visualizations.py
"""

import json
import os
from collections import defaultdict
from datetime import datetime

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd
import seaborn as sns

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "output")

# ---------------------------------------------------------------------------
# Styling
# ---------------------------------------------------------------------------
sns.set_theme(style="whitegrid", palette="husl")
plt.rcParams.update({
    "figure.dpi": 150,
    "savefig.dpi": 150,
    "font.size": 10,
    "axes.titlesize": 13,
    "axes.labelsize": 11,
})

CATEGORY_COLORS = sns.color_palette("husl", 15)


# ---------------------------------------------------------------------------
# Data loading — MongoDB with JSON fallback
# ---------------------------------------------------------------------------

def _load_json(filename: str) -> list[dict]:
    path = os.path.join(DATA_DIR, filename)
    with open(path) as f:
        return json.load(f)


def _load_sessions_json() -> list[dict]:
    sessions = []
    i = 0
    while True:
        path = os.path.join(DATA_DIR, f"sessions_{i}.json")
        if not os.path.exists(path):
            break
        sessions.extend(_load_json(f"sessions_{i}.json"))
        i += 1
    return sessions


def load_data() -> dict:
    """Try MongoDB first; fall back to JSON files."""
    try:
        from pymongo import MongoClient

        client = MongoClient(os.environ.get("MONGO_HOST", "localhost"), 27017, serverSelectionTimeoutMS=3000)
        client.server_info()  # trigger connection check
        db = client["ecommerce"]

        products = list(db.products.find())
        users = list(db.users.find())
        transactions = list(db.transactions.find())
        client.close()

        # Sessions are in HBase, not MongoDB — always load from JSON
        sessions = _load_sessions_json()

        print("Data loaded from MongoDB (sessions from JSON files).")
        return {
            "products": products,
            "users": users,
            "transactions": transactions,
            "sessions": sessions,
            "categories": _load_json("categories.json"),
        }
    except Exception as e:
        print(f"MongoDB unavailable ({e}), loading from JSON files...")
        return {
            "categories": _load_json("categories.json"),
            "products": _load_json("products.json"),
            "users": _load_json("users.json"),
            "transactions": _load_json("transactions.json"),
            "sessions": _load_sessions_json(),
        }


# ---------------------------------------------------------------------------
# Helper: build lookups
# ---------------------------------------------------------------------------

def _build_product_category_map(products: list[dict], categories: list[dict]) -> dict:
    """Map product_id -> category name."""
    # If loaded from MongoDB, products have embedded 'category.name'
    # If loaded from JSON, we need to join via category_id
    cat_id_to_name = {}
    for cat in categories:
        cat_id_to_name[cat.get("category_id", cat.get("_id", ""))] = cat["name"]

    mapping = {}
    for p in products:
        pid = p.get("product_id", p.get("_id"))
        # MongoDB structure: category is embedded dict
        if isinstance(p.get("category"), dict) and "name" in p["category"]:
            mapping[pid] = p["category"]["name"]
        else:
            mapping[pid] = cat_id_to_name.get(p.get("category_id", ""), "Unknown")
    return mapping


def _build_product_name_map(products: list[dict]) -> dict:
    """Map product_id -> product name."""
    result = {}
    for p in products:
        pid = p.get("product_id", p.get("_id"))
        result[pid] = p["name"]
    return result


# ---------------------------------------------------------------------------
# Visualization 1: Revenue Over Time by Category
# ---------------------------------------------------------------------------

def viz_revenue_trends(data: dict) -> None:
    """Weekly revenue trend over the 90-day period, multi-line by top 5 categories."""
    print("\n--- Visualization 1: Revenue Over Time by Category ---")

    prod_cat = _build_product_category_map(data["products"], data["categories"])

    # Build daily revenue per category
    daily: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for txn in data["transactions"]:
        date_str = txn["timestamp"][:10]
        for item in txn["items"]:
            cat = prod_cat.get(item["product_id"], "Unknown")
            daily[date_str][cat] += item["subtotal"]

    # Convert to DataFrame
    rows = []
    for date_str, cats in daily.items():
        for cat, revenue in cats.items():
            rows.append({"date": pd.to_datetime(date_str), "category": cat, "revenue": revenue})
    df = pd.DataFrame(rows)

    if df.empty:
        print("  No transaction data found. Skipping.")
        return

    # Identify top 5 categories by total revenue
    top5 = df.groupby("category")["revenue"].sum().nlargest(5).index.tolist()

    # Weekly aggregation
    df["week"] = df["date"].dt.to_period("W").apply(lambda p: p.start_time)
    weekly = df[df["category"].isin(top5)].groupby(["week", "category"])["revenue"].sum().reset_index()

    # Overall weekly trend (all categories)
    overall_weekly = df.groupby("week")["revenue"].sum().reset_index()

    # Plot
    fig, ax = plt.subplots(figsize=(12, 6))
    palette = sns.color_palette("husl", len(top5))

    for i, cat in enumerate(top5):
        subset = weekly[weekly["category"] == cat].sort_values("week")
        ax.plot(subset["week"], subset["revenue"], marker="o", markersize=4,
                linewidth=2, label=cat, color=palette[i])

    # Overall trend as dashed line
    overall_weekly = overall_weekly.sort_values("week")
    ax.plot(overall_weekly["week"], overall_weekly["revenue"], linestyle="--",
            linewidth=2, color="gray", alpha=0.7, label="Overall (all categories)")

    ax.set_title("Weekly Revenue Trends by Top 5 Categories", fontweight="bold", pad=12)
    ax.set_xlabel("Week Starting")
    ax.set_ylabel("Revenue (RWF)")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"RWF {x:,.0f}"))
    ax.legend(loc="upper left", fontsize=9, framealpha=0.9)
    fig.autofmt_xdate(rotation=30)
    plt.tight_layout()

    path = os.path.join(OUTPUT_DIR, "viz_revenue_trends.png")
    fig.savefig(path)
    plt.close(fig)
    print(f"  Saved: {path}")

    # Business insight
    total_by_cat = df.groupby("category")["revenue"].sum().sort_values(ascending=False)
    top_cat = total_by_cat.index[0]
    top_rev = total_by_cat.iloc[0]
    overall_total = total_by_cat.sum()
    print(f"  INSIGHT: '{top_cat}' leads revenue at RWF {top_rev:,.0f} "
          f"({top_rev / overall_total * 100:.1f}% of total RWF {overall_total:,.0f}). "
          f"The top 5 categories account for "
          f"{total_by_cat.head(5).sum() / overall_total * 100:.1f}% of all revenue, "
          f"suggesting a concentrated revenue distribution across product categories.")


# ---------------------------------------------------------------------------
# Visualization 2: Customer Segmentation
# ---------------------------------------------------------------------------

def viz_customer_segments(data: dict) -> None:
    """Customer segmentation: spending tiers by province with scatter overlay."""
    print("\n--- Visualization 2: Customer Segmentation ---")

    rows = []
    for u in data["users"]:
        uid = u.get("user_id", u.get("_id"))
        geo = u.get("geo_data", {})
        province = geo.get("province", "Unknown")
        summary = u.get("purchase_summary", {})
        total_spent = summary.get("total_spent", 0.0)
        total_orders = summary.get("total_orders", 0)
        rows.append({
            "user_id": uid,
            "province": province,
            "total_spent": total_spent,
            "total_orders": total_orders,
        })

    # If loaded from JSON (no purchase_summary), compute from transactions
    if all(r["total_spent"] == 0 for r in rows):
        user_spending: dict[str, dict] = defaultdict(lambda: {"total_spent": 0.0, "total_orders": 0})
        for txn in data["transactions"]:
            uid = txn.get("user_id", "")
            user_spending[uid]["total_spent"] += txn.get("total", 0.0)
            user_spending[uid]["total_orders"] += 1
        for r in rows:
            if r["user_id"] in user_spending:
                r["total_spent"] = user_spending[r["user_id"]]["total_spent"]
                r["total_orders"] = user_spending[r["user_id"]]["total_orders"]

    df = pd.DataFrame(rows)

    # Spending tier classification
    def classify_tier(spent: float, orders: int) -> str:
        if orders == 0:
            return "Inactive"
        if spent >= 5000:
            return "VIP"
        if spent >= 2000:
            return "Regular"
        return "Low"

    df["tier"] = df.apply(lambda r: classify_tier(r["total_spent"], r["total_orders"]), axis=1)

    tier_order = ["VIP", "Regular", "Low", "Inactive"]
    province_order = sorted(df["province"].unique())

    # Create figure with two subplots
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6), gridspec_kw={"width_ratios": [1, 1.2]})

    # Left: scatter plot (active users only)
    active = df[df["total_orders"] > 0].copy()
    province_palette = sns.color_palette("husl", len(province_order))
    prov_color_map = dict(zip(province_order, province_palette))

    for prov in province_order:
        subset = active[active["province"] == prov]
        ax1.scatter(subset["total_orders"], subset["total_spent"],
                    alpha=0.6, s=30, label=prov, color=prov_color_map[prov], edgecolors="white",
                    linewidths=0.3)

    ax1.set_title("User Spending vs Orders by Province", fontweight="bold")
    ax1.set_xlabel("Total Orders")
    ax1.set_ylabel("Total Spent (RWF)")
    ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"RWF {x:,.0f}"))
    ax1.legend(fontsize=8, title="Province", title_fontsize=9, loc="upper left")

    # Right: stacked bar chart of tiers by province
    tier_prov = df.groupby(["province", "tier"]).size().unstack(fill_value=0)
    tier_prov = tier_prov.reindex(columns=tier_order, fill_value=0)

    tier_colors = {"VIP": "#2ecc71", "Regular": "#3498db", "Low": "#f39c12", "Inactive": "#95a5a6"}
    tier_prov.plot(kind="barh", stacked=True, ax=ax2,
                   color=[tier_colors[t] for t in tier_prov.columns], edgecolor="white", linewidth=0.5)

    ax2.set_title("Customer Tiers by Province", fontweight="bold")
    ax2.set_xlabel("Number of Users")
    ax2.set_ylabel("")
    ax2.legend(title="Tier", fontsize=9, title_fontsize=9, loc="lower right")

    plt.tight_layout()
    path = os.path.join(OUTPUT_DIR, "viz_customer_segments.png")
    fig.savefig(path)
    plt.close(fig)
    print(f"  Saved: {path}")

    # Business insight
    tier_counts = df["tier"].value_counts()
    vip_count = tier_counts.get("VIP", 0)
    inactive_count = tier_counts.get("Inactive", 0)
    vip_rev = df[df["tier"] == "VIP"]["total_spent"].sum()
    total_rev = df["total_spent"].sum()
    print(f"  INSIGHT: {vip_count} VIP customers ({vip_count / len(df) * 100:.1f}% of users) "
          f"generate RWF {vip_rev:,.0f} ({vip_rev / total_rev * 100:.1f}% of revenue). "
          f"{inactive_count} users ({inactive_count / len(df) * 100:.1f}%) have made no purchases, "
          f"representing a significant re-engagement opportunity.")


# ---------------------------------------------------------------------------
# Visualization 3: Product Performance Dashboard
# ---------------------------------------------------------------------------

def viz_product_performance(data: dict) -> None:
    """Top 15 products by revenue vs quantity sold (horizontal bar chart)."""
    print("\n--- Visualization 3: Product Performance Dashboard ---")

    prod_names = _build_product_name_map(data["products"])
    prod_cats = _build_product_category_map(data["products"], data["categories"])

    # Aggregate product-level metrics from transactions
    product_stats: dict[str, dict] = defaultdict(lambda: {"revenue": 0.0, "quantity": 0})
    for txn in data["transactions"]:
        for item in txn["items"]:
            pid = item["product_id"]
            product_stats[pid]["revenue"] += item["subtotal"]
            product_stats[pid]["quantity"] += item["quantity"]

    rows = []
    for pid, stats in product_stats.items():
        name = prod_names.get(pid, pid)
        cat = prod_cats.get(pid, "Unknown")
        # Truncate long product names
        label = f"{name[:25]}{'...' if len(name) > 25 else ''} [{cat}]"
        rows.append({
            "product_id": pid,
            "label": label,
            "category": cat,
            "revenue": stats["revenue"],
            "quantity": stats["quantity"],
        })

    df = pd.DataFrame(rows)

    if df.empty:
        print("  No product performance data. Skipping.")
        return

    # Top 15 by revenue
    top_rev = df.nlargest(15, "revenue").sort_values("revenue", ascending=True)
    # Top 15 by quantity
    top_qty = df.nlargest(15, "quantity").sort_values("quantity", ascending=True)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 8))

    # Revenue bars
    colors_rev = [sns.color_palette("husl", 15)[i % 15] for i in range(len(top_rev))]
    ax1.barh(top_rev["label"], top_rev["revenue"], color=colors_rev, edgecolor="white", linewidth=0.5)
    ax1.set_title("Top 15 Products by Revenue", fontweight="bold")
    ax1.set_xlabel("Revenue (RWF)")
    ax1.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"RWF {x:,.0f}"))

    # Annotate revenue values
    for i, (_, row) in enumerate(top_rev.iterrows()):
        ax1.text(row["revenue"] + max(top_rev["revenue"]) * 0.01, i,
                 f"RWF {row['revenue']:,.0f}", va="center", fontsize=8)

    # Quantity bars
    colors_qty = [sns.color_palette("husl", 15)[i % 15] for i in range(len(top_qty))]
    ax2.barh(top_qty["label"], top_qty["quantity"], color=colors_qty, edgecolor="white", linewidth=0.5)
    ax2.set_title("Top 15 Products by Quantity Sold", fontweight="bold")
    ax2.set_xlabel("Units Sold")

    # Annotate quantity values
    for i, (_, row) in enumerate(top_qty.iterrows()):
        ax2.text(row["quantity"] + max(top_qty["quantity"]) * 0.01, i,
                 f"{row['quantity']}", va="center", fontsize=8)

    plt.tight_layout()
    path = os.path.join(OUTPUT_DIR, "viz_product_performance.png")
    fig.savefig(path)
    plt.close(fig)
    print(f"  Saved: {path}")

    # Business insight
    top1_rev = df.nlargest(1, "revenue").iloc[0]
    top1_qty = df.nlargest(1, "quantity").iloc[0]
    total_products_sold = len(product_stats)
    top15_rev_share = df.nlargest(15, "revenue")["revenue"].sum() / df["revenue"].sum() * 100
    print(f"  INSIGHT: The highest-grossing product is '{prod_names.get(top1_rev['product_id'], 'N/A')}' "
          f"(RWF {top1_rev['revenue']:,.0f}), while the most popular by volume is "
          f"'{prod_names.get(top1_qty['product_id'], 'N/A')}' ({top1_qty['quantity']} units). "
          f"The top 15 products by revenue account for {top15_rev_share:.1f}% of total revenue "
          f"across {total_products_sold} distinct products sold.")


# ---------------------------------------------------------------------------
# Visualization 4: Conversion Funnel
# ---------------------------------------------------------------------------

def viz_conversion_funnel(data: dict) -> None:
    """Conversion funnel stages broken down by device type."""
    print("\n--- Visualization 4: Conversion Funnel ---")

    sessions = data["sessions"]

    # Funnel stages per device type
    device_funnel: dict[str, dict[str, int]] = defaultdict(
        lambda: {"Total Sessions": 0, "Viewed Products": 0, "Added to Cart": 0, "Converted": 0}
    )

    for s in sessions:
        device_type = s.get("device_profile", {}).get("type", "unknown")
        device_funnel[device_type]["Total Sessions"] += 1

        viewed = s.get("viewed_products", [])
        if viewed:
            device_funnel[device_type]["Viewed Products"] += 1

        cart = s.get("cart_contents", {})
        if cart:
            device_funnel[device_type]["Added to Cart"] += 1

        if s.get("conversion_status") == "converted":
            device_funnel[device_type]["Converted"] += 1

    stages = ["Total Sessions", "Viewed Products", "Added to Cart", "Converted"]
    devices = sorted(device_funnel.keys())

    # Overall funnel totals
    overall = {stage: sum(device_funnel[d][stage] for d in devices) for stage in stages}

    # Create figure: left = overall funnel, right = device breakdown
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 7), gridspec_kw={"width_ratios": [1, 1.3]})

    # Left: overall funnel (horizontal bars, widest at top)
    stage_values = [overall[s] for s in stages]
    colors_funnel = ["#3498db", "#2ecc71", "#f39c12", "#e74c3c"]

    y_pos = list(range(len(stages) - 1, -1, -1))
    bars = ax1.barh(y_pos, stage_values, color=colors_funnel, edgecolor="white", linewidth=0.8, height=0.7)
    ax1.set_yticks(y_pos)
    ax1.set_yticklabels(stages)
    ax1.set_title("Overall Conversion Funnel", fontweight="bold")
    ax1.set_xlabel("Number of Sessions")

    # Annotate with counts and percentages
    for i, (stage, val) in enumerate(zip(stages, stage_values)):
        pct = val / stage_values[0] * 100
        ax1.text(val + max(stage_values) * 0.02, y_pos[i],
                 f"{val:,} ({pct:.1f}%)", va="center", fontsize=9, fontweight="bold")

    # Right: grouped bars by device type
    x = range(len(stages))
    bar_width = 0.8 / len(devices)
    device_colors = sns.color_palette("husl", len(devices))

    for i, device in enumerate(devices):
        values = [device_funnel[device][s] for s in stages]
        offsets = [xi + i * bar_width for xi in x]
        ax2.bar(offsets, values, bar_width, label=device.capitalize(),
                color=device_colors[i], edgecolor="white", linewidth=0.5)

    ax2.set_xticks([xi + bar_width * (len(devices) - 1) / 2 for xi in x])
    ax2.set_xticklabels(stages, rotation=15, ha="right")
    ax2.set_title("Conversion Funnel by Device Type", fontweight="bold")
    ax2.set_ylabel("Number of Sessions")
    ax2.legend(title="Device", fontsize=9, title_fontsize=9)

    plt.tight_layout()
    path = os.path.join(OUTPUT_DIR, "viz_conversion_funnel.png")
    fig.savefig(path)
    plt.close(fig)
    print(f"  Saved: {path}")

    # Business insight
    overall_conv_rate = overall["Converted"] / overall["Total Sessions"] * 100
    cart_to_conv = overall["Converted"] / overall["Added to Cart"] * 100 if overall["Added to Cart"] else 0
    best_device = max(devices, key=lambda d: (
        device_funnel[d]["Converted"] / device_funnel[d]["Total Sessions"]
        if device_funnel[d]["Total Sessions"] > 0 else 0
    ))
    best_rate = device_funnel[best_device]["Converted"] / device_funnel[best_device]["Total Sessions"] * 100
    cart_abandon_rate = (1 - overall["Converted"] / overall["Added to Cart"]) * 100 if overall["Added to Cart"] else 0
    print(f"  INSIGHT: Overall conversion rate is {overall_conv_rate:.1f}% "
          f"({overall['Converted']:,} of {overall['Total Sessions']:,} sessions). "
          f"Cart-to-purchase rate is {cart_to_conv:.1f}%, meaning {cart_abandon_rate:.1f}% "
          f"of carts are abandoned. '{best_device.capitalize()}' devices convert best at "
          f"{best_rate:.1f}%, suggesting device-specific UX optimization opportunities.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 60)
    print("E-Commerce Analytics — Part 4: Visualizations & Insights")
    print("=" * 60)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    data = load_data()

    viz_revenue_trends(data)
    viz_customer_segments(data)
    viz_product_performance(data)
    viz_conversion_funnel(data)

    print("\n" + "=" * 60)
    print(f"All visualizations saved to: {OUTPUT_DIR}/")
    print("  - viz_revenue_trends.png")
    print("  - viz_customer_segments.png")
    print("  - viz_product_performance.png")
    print("  - viz_conversion_funnel.png")
    print("=" * 60)


if __name__ == "__main__":
    main()
