"""
Interactive E-Commerce Analytics Dashboard (Streamlit + Plotly).

Connects to MongoDB for products/users/transactions and loads
session data from HBase (with JSON fallback). Provides real-time
exploration of all four project parts.

Usage: streamlit run src/dashboard.py
"""

import json
import os
from collections import defaultdict
from datetime import datetime

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
from plotly.subplots import make_subplots
import streamlit as st

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
HBASE_HOST = os.environ.get("HBASE_HOST", "localhost")
HBASE_PORT = int(os.environ.get("HBASE_PORT", "9090"))

st.set_page_config(
    page_title="E-Commerce Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Theme & Styling
# ---------------------------------------------------------------------------

THEME = {
    "bg": "rgba(0,0,0,0)",
    "card_bg": "#1e1e2e",
    "text": "#cdd6f4",
    "accent": "#89b4fa",
    "success": "#a6e3a1",
    "warning": "#f9e2af",
    "error": "#f38ba8",
    "grid": "rgba(205,214,244,0.08)",
    "font": "Inter, sans-serif",
}

CATEGORY_COLORS = [
    "#89b4fa", "#a6e3a1", "#f9e2af", "#f38ba8", "#cba6f7",
    "#94e2d5", "#fab387", "#74c7ec", "#f5c2e7", "#b4befe",
    "#eba0ac", "#89dceb", "#f5e0dc", "#a6adc8", "#585b70",
]

TIER_COLORS = {
    "VIP": "#a6e3a1",
    "Regular": "#89b4fa",
    "Low": "#f9e2af",
    "Inactive": "#585b70",
}

STATUS_COLORS = {
    "completed": "#a6e3a1",
    "shipped": "#89b4fa",
    "processing": "#f9e2af",
    "refunded": "#f38ba8",
}

# Register custom Plotly template — uses transparent bg so it adapts to light/dark
_template = go.layout.Template()
_template.layout = go.Layout(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family=THEME["font"], size=13),
    title=dict(font=dict(size=16), x=0, xanchor="left"),
    xaxis=dict(gridcolor="rgba(128,128,128,0.15)", zerolinecolor="rgba(128,128,128,0.15)"),
    yaxis=dict(gridcolor="rgba(128,128,128,0.15)", zerolinecolor="rgba(128,128,128,0.15)"),
    legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(size=11)),
    colorway=CATEGORY_COLORS,
    hoverlabel=dict(font_size=12),
    margin=dict(l=40, r=20, t=50, b=40),
)
pio.templates["ecommerce_dark"] = _template
pio.templates.default = "ecommerce_dark"


def inject_custom_css():
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

    .stApp {
        font-family: 'Inter', sans-serif;
    }

    /* Theme-adaptive CSS variables */
    :root {
        --card-bg: rgba(0,0,0,0.04);
        --card-border: rgba(100,100,120,0.15);
        --card-shadow: rgba(0,0,0,0.06);
        --card-inset: rgba(255,255,255,0.5);
        --sidebar-bg: transparent;
    }

    @media (prefers-color-scheme: dark) {
        :root {
            --card-bg: rgba(255,255,255,0.04);
            --card-border: rgba(137,180,250,0.2);
            --card-shadow: rgba(0,0,0,0.3);
            --card-inset: rgba(137,180,250,0.1);
            --sidebar-bg: transparent;
        }
    }

    /* Streamlit dark theme detection */
    [data-testid="stAppViewContainer"][data-theme="dark"] {
        --card-bg: rgba(255,255,255,0.04);
        --card-border: rgba(137,180,250,0.2);
        --card-shadow: rgba(0,0,0,0.3);
        --card-inset: rgba(137,180,250,0.1);
    }

    [data-testid="stMetric"] {
        background: var(--card-bg);
        border: 1px solid var(--card-border);
        border-radius: 12px;
        padding: 16px 20px;
        box-shadow: 0 4px 16px var(--card-shadow), inset 0 1px 0 var(--card-inset);
    }

    [data-testid="stMetric"] label {
        font-size: 0.85rem !important;
        font-weight: 500 !important;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        opacity: 0.7;
    }

    [data-testid="stMetric"] [data-testid="stMetricValue"] {
        font-weight: 700 !important;
    }

    [data-testid="stPlotlyChart"] {
        background: var(--card-bg);
        border: 1px solid var(--card-border);
        border-radius: 12px;
        padding: 8px;
        box-shadow: 0 2px 12px var(--card-shadow);
    }

    .stDataFrame {
        border: 1px solid var(--card-border);
        border-radius: 8px;
        overflow: hidden;
    }

    h1, h2, h3 {
        font-weight: 600 !important;
    }

    hr {
        border-color: var(--card-border) !important;
    }
    </style>
    """, unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Data loading (cached)
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300)
def load_data():
    """Load data from MongoDB with JSON fallback."""
    try:
        from pymongo import MongoClient
        client = MongoClient(os.environ.get("MONGO_HOST", "localhost"), 27017, serverSelectionTimeoutMS=3000)
        client.server_info()
        db = client["ecommerce"]
        products = list(db.products.find())
        users = list(db.users.find())
        transactions = list(db.transactions.find())
        client.close()
        source = "MongoDB"
    except Exception:
        products = _load_json("products.json")
        users = _load_json("users.json")
        transactions = _load_json("transactions.json")
        source = "JSON files"

    sessions, session_source = _load_sessions_hbase()
    if not sessions:
        sessions = _load_sessions_json()
        session_source = "JSON files"
    else:
        # HBase may not store page_views — supplement from JSON fallback
        json_sessions = _load_sessions_json()
        if json_sessions:
            json_map = {s["session_id"]: s for s in json_sessions}
            for s in sessions:
                if not s.get("page_views"):
                    js = json_map.get(s["session_id"], {})
                    s["page_views"] = js.get("page_views", [])
                if not s.get("cart_contents"):
                    js = json_map.get(s["session_id"], {})
                    s["cart_contents"] = js.get("cart_contents", {})
    categories = _load_json("categories.json")
    return products, users, transactions, sessions, categories, source, session_source


def _load_json(filename):
    with open(os.path.join(DATA_DIR, filename)) as f:
        return json.load(f)


def _load_sessions_json():
    sessions = []
    i = 0
    while True:
        path = os.path.join(DATA_DIR, f"sessions_{i}.json")
        if not os.path.exists(path):
            break
        with open(path) as f:
            sessions.extend(json.load(f))
        i += 1
    return sessions


def _load_sessions_hbase():
    """Load session data from HBase. Returns (sessions_list, source_label) or ([], None) on failure."""
    try:
        import happybase
        conn = happybase.Connection(HBASE_HOST, HBASE_PORT, timeout=5000)
        table = conn.table("sessions")

        sessions = []
        for _row_key, data in table.scan():
            row = {k.decode("utf-8"): v.decode("utf-8") for k, v in data.items()}
            sessions.append({
                "session_id": row.get("info:session_id", ""),
                "user_id": row.get("info:user_id", ""),
                "start_time": row.get("info:start_time", ""),
                "duration_seconds": int(row.get("info:duration_seconds", "0")),
                "conversion_status": row.get("info:conversion_status", ""),
                "referrer": row.get("info:referrer", ""),
                "device_profile": {
                    "type": row.get("device:type", ""),
                    "os": row.get("device:os", ""),
                    "browser": row.get("device:browser", ""),
                },
                "geo_data": {
                    "city": row.get("geo:city", ""),
                    "state": row.get("geo:state", ""),
                },
                "viewed_products": json.loads(row.get("activity:viewed_products", "[]")),
                "page_views": json.loads(row.get("activity:page_views", "[]")),
                "cart_contents": json.loads(row.get("activity:cart_contents", "{}")),
            })

        conn.close()
        if sessions:
            return sessions, "HBase"
        return [], None
    except Exception:
        return [], None


# ---------------------------------------------------------------------------
# Data preparation helpers
# ---------------------------------------------------------------------------

def build_product_map(products, categories):
    cat_map = {}
    subcat_map = {}
    for c in categories:
        cid = c.get("category_id", c.get("_id", ""))
        cat_map[cid] = c["name"]
        for sc in c.get("subcategories", []):
            subcat_map[sc["subcategory_id"]] = {
                "name": sc["name"],
                "profit_margin": sc.get("profit_margin", 0),
                "category": c["name"],
                "category_id": cid,
            }

    prod_info = {}
    for p in products:
        pid = p.get("product_id", p.get("_id"))
        if isinstance(p.get("category"), dict) and "name" in p["category"]:
            cat_name = p["category"]["name"]
        else:
            cat_name = cat_map.get(p.get("category_id", ""), "Unknown")
        sc_id = p.get("subcategory_id", "")
        sc_info = subcat_map.get(sc_id, {})
        prod_info[pid] = {
            "name": p["name"],
            "category": cat_name,
            "base_price": p.get("base_price", 0),
            "subcategory_id": sc_id,
            "subcategory_name": sc_info.get("name", "Unknown"),
            "profit_margin": sc_info.get("profit_margin", 0),
        }
    return prod_info


def prepare_transactions_df(transactions, prod_info):
    rows = []
    for t in transactions:
        ts = t.get("timestamp", "")
        for item in t.get("items", []):
            pid = item["product_id"]
            info = prod_info.get(pid, {"name": pid, "category": "Unknown", "base_price": 0,
                                        "subcategory_name": "Unknown", "profit_margin": 0})
            rows.append({
                "date": ts[:10],
                "user_id": t.get("user_id"),
                "transaction_id": t.get("transaction_id"),
                "product_id": pid,
                "product_name": info["name"],
                "category": info["category"],
                "subcategory": info.get("subcategory_name", "Unknown"),
                "profit_margin": info.get("profit_margin", 0),
                "quantity": item["quantity"],
                "unit_price": item["unit_price"],
                "subtotal": item["subtotal"],
                "total": t.get("total", 0),
                "discount": t.get("discount", 0),
                "payment_method": t.get("payment_method", ""),
                "status": t.get("status", ""),
            })
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    return df


def prepare_users_df(users):
    rows = []
    for u in users:
        uid = u.get("user_id", u.get("_id"))
        geo = u.get("geo_data", {})
        ps = u.get("purchase_summary", {})
        rows.append({
            "user_id": uid,
            "name": u.get("name", ""),
            "province": geo.get("province", "Unknown"),
            "city": geo.get("city", "Unknown"),
            "registration_date": u.get("registration_date", ""),
            "total_orders": ps.get("total_orders", 0),
            "total_spent": ps.get("total_spent", 0.0),
            "avg_order_value": ps.get("avg_order_value", 0.0),
        })
    df = pd.DataFrame(rows)
    df["registration_date"] = pd.to_datetime(df["registration_date"], errors="coerce")
    return df


def prepare_sessions_df(sessions):
    rows = []
    for s in sessions:
        rows.append({
            "session_id": s["session_id"],
            "user_id": s["user_id"],
            "start_time": s["start_time"],
            "duration_seconds": s["duration_seconds"],
            "conversion_status": s["conversion_status"],
            "referrer": s["referrer"],
            "device_type": s["device_profile"]["type"],
            "device_os": s["device_profile"]["os"],
            "browser": s["device_profile"]["browser"],
            "city": s["geo_data"]["city"],
            "province": s["geo_data"]["state"],
            "viewed_products": len(s.get("viewed_products", [])),
            "cart_items": len(s.get("cart_contents", {})),
        })
    df = pd.DataFrame(rows)
    df["start_time"] = pd.to_datetime(df["start_time"], errors="coerce")
    df["date"] = df["start_time"].dt.date
    df["hour"] = df["start_time"].dt.hour
    return df


def prepare_page_views_df(sessions):
    rows = []
    for s in sessions:
        sid = s["session_id"]
        for pv in s.get("page_views", []):
            rows.append({
                "session_id": sid,
                "page_type": pv.get("page_type", "unknown"),
                "view_duration": pv.get("view_duration", 0),
                "timestamp": pv.get("timestamp", ""),
                "product_id": pv.get("product_id"),
            })
    df = pd.DataFrame(rows)
    if not df.empty:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        df["date"] = df["timestamp"].dt.date
    return df


def prepare_cart_df(sessions, prod_info):
    rows = []
    for s in sessions:
        sid = s["session_id"]
        conv = s["conversion_status"]
        for pid, info in s.get("cart_contents", {}).items():
            pi = prod_info.get(pid, {"name": pid, "category": "Unknown"})
            rows.append({
                "session_id": sid,
                "product_id": pid,
                "product_name": pi["name"],
                "category": pi["category"],
                "quantity": info.get("quantity", 0),
                "price": info.get("price", 0),
                "line_total": info.get("quantity", 0) * info.get("price", 0),
                "conversion_status": conv,
            })
    return pd.DataFrame(rows)


def prepare_price_history_df(products, prod_info):
    rows = []
    for p in products:
        pid = p.get("product_id", p.get("_id"))
        pi = prod_info.get(pid, {"name": pid})
        for ph in p.get("price_history", []):
            rows.append({
                "product_id": pid,
                "product_name": pi["name"],
                "price": ph["price"],
                "date": ph["date"][:10],
            })
    df = pd.DataFrame(rows)
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])
    return df


def prepare_subcategory_df(categories, txn_df, products):
    subcat_info = {}
    for c in categories:
        for sc in c.get("subcategories", []):
            subcat_info[sc["subcategory_id"]] = {
                "category": c["name"],
                "subcategory": sc["name"],
                "profit_margin": sc.get("profit_margin", 0),
            }

    prod_subcat = {}
    for p in products:
        pid = p.get("product_id", p.get("_id"))
        prod_subcat[pid] = p.get("subcategory_id", "")

    rows = []
    if "product_id" in txn_df.columns:
        txn_copy = txn_df.copy()
        txn_copy["subcategory_id"] = txn_copy["product_id"].map(prod_subcat)
        grouped = txn_copy.groupby("subcategory_id")["subtotal"].sum().reset_index()
        for _, row in grouped.iterrows():
            sc_id = row["subcategory_id"]
            info = subcat_info.get(sc_id, {"category": "Unknown", "subcategory": "Unknown", "profit_margin": 0})
            rows.append({
                "category": info["category"],
                "subcategory": info["subcategory"],
                "profit_margin": info["profit_margin"],
                "revenue": row["subtotal"],
            })
    return pd.DataFrame(rows)


def prepare_cohort_df(users_df, txn_df):
    cohort_users = users_df[["user_id", "registration_date"]].copy()
    cohort_users["cohort_month"] = cohort_users["registration_date"].dt.to_period("M").astype(str)

    txn_with_cohort = txn_df[["user_id", "date", "subtotal"]].merge(cohort_users, on="user_id", how="left")
    txn_with_cohort["activity_month"] = txn_with_cohort["date"].dt.to_period("M").astype(str)

    pivot = txn_with_cohort.groupby(["cohort_month", "activity_month"])["subtotal"].sum().reset_index()
    pivot = pivot.pivot(index="cohort_month", columns="activity_month", values="subtotal").fillna(0)
    return pivot


def compute_page_sequences(sessions):
    seq_counts = defaultdict(int)
    for s in sessions:
        pvs = s.get("page_views", [])
        page_types = [pv.get("page_type", "unknown") for pv in pvs]
        for i in range(len(page_types) - 1):
            seq_counts[(page_types[i], page_types[i + 1])] += 1

    rows = [{"source": k[0], "target": k[1], "count": v} for k, v in seq_counts.items()]
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Dashboard pages
# ---------------------------------------------------------------------------

def page_overview(txn_df, users_df, sessions_df, transactions):
    st.header("Overview")

    if txn_df.empty:
        st.warning("No transaction data matches the selected filters.")
        return

    # KPI metrics with deltas
    total_rev = txn_df["subtotal"].sum()
    cutoff = txn_df["date"].max() - pd.Timedelta(days=30)
    cutoff_prior = cutoff - pd.Timedelta(days=30)
    last_30 = txn_df[txn_df["date"] >= cutoff]["subtotal"].sum()
    prior_30 = txn_df[(txn_df["date"] >= cutoff_prior) & (txn_df["date"] < cutoff)]["subtotal"].sum()
    rev_delta = last_30 - prior_30

    total_users = users_df["user_id"].nunique()
    total_products = txn_df["product_id"].nunique()
    total_sessions = len(sessions_df)
    conv_rate = (sessions_df["conversion_status"] == "converted").mean() * 100

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total Revenue", f"RWF {total_rev:,.0f}", delta=f"RWF {rev_delta:+,.0f} (30d)")
    c2.metric("Unique Customers", f"{total_users:,}")
    c3.metric("Products Sold", f"{total_products:,}")
    c4.metric("Sessions", f"{total_sessions:,}")
    c5.metric("Conversion Rate", f"{conv_rate:.1f}%")

    st.markdown("---")

    # Revenue Gauge + Daily Revenue
    col1, col2 = st.columns(2)

    with col1:
        target = total_rev * 1.2
        fig = go.Figure(go.Indicator(
            mode="gauge+number+delta",
            value=total_rev,
            delta={"reference": target, "valueformat": ",.0f"},
            title={"text": "Revenue vs Target", "font": {"size": 14}},
            number={"prefix": "RWF ", "valueformat": ",.0f"},
            gauge={
                "axis": {"range": [0, target * 1.1], "tickformat": ",.0f"},
                "bar": {"color": THEME["accent"]},
                "steps": [
                    {"range": [0, target * 0.5], "color": "rgba(243,139,168,0.2)"},
                    {"range": [target * 0.5, target * 0.8], "color": "rgba(249,226,175,0.2)"},
                    {"range": [target * 0.8, target * 1.1], "color": "rgba(166,227,161,0.2)"},
                ],
                "threshold": {"line": {"color": THEME["success"], "width": 3}, "thickness": 0.8, "value": target},
            },
        ))
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        daily = txn_df.groupby("date")["subtotal"].sum().reset_index()
        daily.columns = ["date", "revenue"]
        daily = daily.sort_values("date")
        daily["7d_avg"] = daily["revenue"].rolling(7, min_periods=1).mean()

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=daily["date"], y=daily["revenue"], name="Daily Revenue",
            fill="tozeroy", fillcolor="rgba(137,180,250,0.15)",
            line=dict(color=THEME["accent"], width=1),
        ))
        fig.add_trace(go.Scatter(
            x=daily["date"], y=daily["7d_avg"], name="7-Day Avg",
            line=dict(color=THEME["error"], width=2.5),
        ))
        fig.update_layout(
            title="Daily Revenue with 7-Day Moving Average",
            xaxis_title="Date", yaxis_title="Revenue (RWF)",
            legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01),
            height=400,
        )
        st.plotly_chart(fig, use_container_width=True)

    col3, col4 = st.columns(2)

    with col3:
        # Revenue by category
        cat_rev = txn_df.groupby("category")["subtotal"].sum().sort_values(ascending=True).reset_index()
        cat_rev.columns = ["category", "revenue"]
        cat_color_map = {cat: CATEGORY_COLORS[i % len(CATEGORY_COLORS)] for i, cat in enumerate(sorted(cat_rev["category"].unique()))}
        fig = px.bar(cat_rev, x="revenue", y="category", orientation="h",
                     title="Revenue by Category",
                     labels={"revenue": "Revenue (RWF)", "category": ""},
                     color="category", color_discrete_map=cat_color_map)
        fig.update_layout(height=400, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    with col4:
        # Revenue Waterfall (month-over-month)
        txn_df["month"] = txn_df["date"].dt.to_period("M").astype(str)
        monthly_rev = txn_df.groupby("month")["subtotal"].sum().reset_index()
        monthly_rev.columns = ["month", "revenue"]
        monthly_rev = monthly_rev.sort_values("month")

        measures = ["absolute"]
        deltas = [monthly_rev.iloc[0]["revenue"]]
        labels = [monthly_rev.iloc[0]["month"]]
        for i in range(1, len(monthly_rev)):
            measures.append("relative")
            deltas.append(monthly_rev.iloc[i]["revenue"] - monthly_rev.iloc[i - 1]["revenue"])
            labels.append(monthly_rev.iloc[i]["month"])
        measures.append("total")
        deltas.append(0)
        labels.append("Total")

        fig = go.Figure(go.Waterfall(
            name="Revenue", orientation="v",
            measure=measures, x=labels, y=deltas,
            connector={"line": {"color": THEME["grid"]}},
            increasing={"marker": {"color": THEME["success"]}},
            decreasing={"marker": {"color": THEME["error"]}},
            totals={"marker": {"color": THEME["accent"]}},
        ))
        fig.update_layout(title="Month-over-Month Revenue Changes", height=400)
        st.plotly_chart(fig, use_container_width=True)

    col5, col6 = st.columns(2)

    with col5:
        # Monthly Revenue & Orders dual-axis
        monthly = txn_df.groupby("month").agg(
            revenue=("subtotal", "sum"),
            orders=("user_id", "count"),
        ).reset_index().sort_values("month")
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(go.Bar(x=monthly["month"], y=monthly["revenue"], name="Revenue",
                             marker_color=THEME["accent"], opacity=0.7), secondary_y=False)
        fig.add_trace(go.Scatter(x=monthly["month"], y=monthly["orders"], name="Order Count",
                                 line=dict(color=THEME["warning"], width=2.5), mode="lines+markers"), secondary_y=True)
        fig.update_layout(title="Monthly Revenue & Order Volume", height=400)
        fig.update_yaxes(title_text="Revenue (RWF)", secondary_y=False)
        fig.update_yaxes(title_text="Orders", secondary_y=True)
        st.plotly_chart(fig, use_container_width=True)

    with col6:
        # Payment Donut + Status Sunburst side by side
        sub1, sub2 = st.columns(2)
        with sub1:
            pay = txn_df.groupby("payment_method")["subtotal"].sum().reset_index()
            pay.columns = ["method", "revenue"]
            fig = px.pie(pay, values="revenue", names="method",
                         title="Payment Methods", hole=0.45,
                         color_discrete_sequence=CATEGORY_COLORS)
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        with sub2:
            # Status -> Payment sunburst
            status_pay = txn_df.groupby(["status", "payment_method"])["subtotal"].sum().reset_index()
            status_pay.columns = ["status", "payment_method", "revenue"]
            fig = px.sunburst(status_pay, path=["status", "payment_method"], values="revenue",
                              title="Status / Payment",
                              color="status", color_discrete_map=STATUS_COLORS)
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)


def page_customers(users_df, txn_df, sessions_df):
    st.header("Customer Analytics")

    # Segmentation
    def classify(row):
        if row["total_orders"] == 0:
            return "Inactive"
        if row["total_spent"] >= 5000:
            return "VIP"
        if row["total_spent"] >= 2000:
            return "Regular"
        return "Low"

    users_df = users_df.copy()
    users_df["tier"] = users_df.apply(classify, axis=1)

    # Inline filter: Tier
    all_tiers = ["VIP", "Regular", "Low", "Inactive"]
    available_tiers = [t for t in all_tiers if t in users_df["tier"].values]
    selected_tiers = st.multiselect("Customer Tier", available_tiers, default=available_tiers)
    users_df = users_df[users_df["tier"].isin(selected_tiers)]
    if users_df.empty:
        st.warning("No customers match the selected filters.")
        return

    col1, col2 = st.columns(2)

    with col1:
        tier_summary = users_df.groupby("tier").agg(
            count=("user_id", "count"),
            avg_spent=("total_spent", "mean"),
            total_revenue=("total_spent", "sum"),
        ).reset_index()
        tier_order = ["VIP", "Regular", "Low", "Inactive"]
        tier_summary["tier"] = pd.Categorical(tier_summary["tier"], categories=tier_order, ordered=True)
        tier_summary = tier_summary.sort_values("tier")
        tier_summary["pct"] = (tier_summary["count"] / tier_summary["count"].sum() * 100).round(1)

        fig = px.bar(tier_summary, x="tier", y="count", color="tier",
                     title="Customer Tier Distribution",
                     color_discrete_map=TIER_COLORS,
                     text=tier_summary.apply(lambda r: f"{r['count']} ({r['pct']}%)", axis=1))
        fig.update_layout(height=400, showlegend=False)
        fig.update_traces(textposition="outside")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        active = users_df[users_df["total_orders"] > 0]
        fig = px.scatter(active, x="total_orders", y="total_spent", color="tier",
                         title="Spending vs Orders by Tier",
                         labels={"total_orders": "Total Orders", "total_spent": "Total Spent (RWF)"},
                         hover_data=["name", "province"],
                         color_discrete_map=TIER_COLORS)
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

    col3, col4 = st.columns(2)

    with col3:
        # Registration Cohort Timeline (monthly registrations by province)
        reg = users_df.dropna(subset=["registration_date"]).copy()
        reg["reg_month"] = reg["registration_date"].dt.to_period("M").astype(str)
        reg_prov = reg.groupby(["reg_month", "province"]).size().reset_index(name="count")
        fig = px.area(reg_prov, x="reg_month", y="count", color="province",
                      title="Registration Timeline by Province",
                      labels={"reg_month": "Month", "count": "New Registrations"},
                      color_discrete_sequence=CATEGORY_COLORS)
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
        st.caption("Stacked area shows registration wave patterns across provinces.")

    with col4:
        # Cohort Spending Heatmap
        cohort = prepare_cohort_df(users_df, txn_df)
        if not cohort.empty:
            fig = px.imshow(cohort, text_auto=".0f", color_continuous_scale="Blues",
                            title="Cohort Spending Heatmap",
                            labels={"x": "Activity Month", "y": "Cohort Month", "color": "Revenue"})
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
            st.caption("Shows how much each registration cohort spends over time.")

    col5, col6 = st.columns(2)

    with col5:
        # Province Revenue Radar
        prov_rev = users_df.groupby("province")["total_spent"].sum().reset_index()
        prov_rev.columns = ["province", "revenue"]
        prov_rev = prov_rev.sort_values("revenue", ascending=False).head(10)
        fig = go.Figure()
        fig.add_trace(go.Scatterpolar(
            r=prov_rev["revenue"].tolist() + [prov_rev["revenue"].iloc[0]],
            theta=prov_rev["province"].tolist() + [prov_rev["province"].iloc[0]],
            fill="toself",
            fillcolor="rgba(137,180,250,0.2)",
            line=dict(color=THEME["accent"], width=2),
            name="Revenue",
        ))
        fig.update_layout(
            title="Top 10 Provinces by Revenue",
            polar=dict(
                bgcolor="rgba(0,0,0,0)",
                radialaxis=dict(gridcolor=THEME["grid"], tickformat=",.0f"),
                angularaxis=dict(gridcolor=THEME["grid"]),
            ),
            height=400,
        )
        st.plotly_chart(fig, use_container_width=True)

    with col6:
        # CLV Distribution (violin by tier)
        active = users_df[users_df["total_orders"] > 0].copy()
        fig = px.violin(active, x="tier", y="total_spent", color="tier",
                        title="Customer Lifetime Value Distribution by Tier",
                        labels={"total_spent": "Total Spent (RWF)", "tier": ""},
                        color_discrete_map=TIER_COLORS, box=True, points="outliers",
                        category_orders={"tier": ["VIP", "Regular", "Low"]})
        fig.update_layout(height=400, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
        st.caption("Violin plot reveals spending distribution within each tier.")

    col7, col8 = st.columns(2)

    with col7:
        # Tier breakdown by province (stacked)
        tier_prov = users_df.groupby(["province", "tier"]).size().reset_index(name="count")
        fig = px.bar(tier_prov, x="province", y="count", color="tier",
                     title="Customer Tiers by Province",
                     color_discrete_map=TIER_COLORS,
                     barmode="stack")
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

    with col8:
        # Top customers table
        st.subheader("Top 15 Customers by Spending")
        top = users_df.nlargest(15, "total_spent")[["name", "province", "total_orders", "total_spent", "avg_order_value", "tier"]]
        top = top.copy()
        top["total_spent"] = top["total_spent"].apply(lambda x: f"RWF {x:,.0f}")
        top["avg_order_value"] = top["avg_order_value"].apply(lambda x: f"RWF {x:,.0f}")
        st.dataframe(top, use_container_width=True, hide_index=True, height=400)


def page_products(txn_df, prod_info, products, categories):
    st.header("Product Analytics")

    if txn_df.empty:
        st.warning("No transaction data matches the selected filters.")
        return

    product_stats = txn_df.groupby(["product_id", "product_name", "category"]).agg(
        revenue=("subtotal", "sum"),
        quantity=("quantity", "sum"),
        orders=("user_id", "count"),
    ).reset_index()

    col1, col2 = st.columns(2)

    with col1:
        top_rev = product_stats.nlargest(15, "revenue").sort_values("revenue", ascending=True)
        cat_color_map = {cat: CATEGORY_COLORS[i % len(CATEGORY_COLORS)] for i, cat in enumerate(sorted(product_stats["category"].unique()))}
        fig = px.bar(top_rev, x="revenue", y="product_name", orientation="h",
                     color="category", title="Top 15 Products by Revenue",
                     labels={"revenue": "Revenue (RWF)", "product_name": ""},
                     hover_data=["quantity", "orders"],
                     color_discrete_map=cat_color_map)
        fig.update_layout(height=500, yaxis=dict(tickfont=dict(size=9)))
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        top_qty = product_stats.nlargest(15, "quantity").sort_values("quantity", ascending=True)
        fig = px.bar(top_qty, x="quantity", y="product_name", orientation="h",
                     color="category", title="Top 15 Products by Quantity",
                     labels={"quantity": "Units Sold", "product_name": ""},
                     hover_data=["revenue", "orders"],
                     color_discrete_map=cat_color_map)
        fig.update_layout(height=500, yaxis=dict(tickfont=dict(size=9)))
        st.plotly_chart(fig, use_container_width=True)

    col3, col4 = st.columns(2)

    with col3:
        # Category -> Subcategory Sunburst
        subcat_df = prepare_subcategory_df(categories, txn_df, products)
        if not subcat_df.empty:
            fig = px.sunburst(subcat_df, path=["category", "subcategory"], values="revenue",
                              title="Category / Subcategory Revenue",
                              color="profit_margin", color_continuous_scale="RdYlGn",
                              color_continuous_midpoint=0.15)
            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)
            st.caption("Color indicates profit margin: green = high, red = low.")

    with col4:
        # Profit Margin by Subcategory
        if not subcat_df.empty:
            top_sub = subcat_df.nlargest(20, "revenue").sort_values("revenue", ascending=True)
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=top_sub["revenue"], y=top_sub["subcategory"],
                orientation="h", name="Revenue",
                marker_color=THEME["accent"], opacity=0.7,
            ))
            fig.add_trace(go.Scatter(
                x=top_sub["profit_margin"] * top_sub["revenue"].max() / top_sub["profit_margin"].max(),
                y=top_sub["subcategory"],
                mode="markers", name="Margin",
                marker=dict(color=THEME["success"], size=10, symbol="diamond"),
            ))
            fig.update_layout(title="Top 20 Subcategories: Revenue & Margin", height=500,
                              yaxis=dict(tickfont=dict(size=9)))
            st.plotly_chart(fig, use_container_width=True)

    col5, col6 = st.columns(2)

    with col5:
        # Price History Timeline (top 10 products)
        price_df = prepare_price_history_df(products, prod_info)
        if not price_df.empty:
            top_pids = product_stats.nlargest(10, "revenue")["product_id"].tolist()
            ph_top = price_df[price_df["product_id"].isin(top_pids)]
            if not ph_top.empty:
                fig = px.line(ph_top, x="date", y="price", color="product_name",
                              title="Price History — Top 10 Products",
                              labels={"price": "Price (RWF)", "date": "Date", "product_name": "Product"},
                              color_discrete_sequence=CATEGORY_COLORS)
                fig.update_layout(height=400)
                fig.update_xaxes(rangeslider_visible=True, rangeslider_thickness=0.05)
                st.plotly_chart(fig, use_container_width=True)

    with col6:
        # Category treemap
        cat_stats = txn_df.groupby("category").agg(
            revenue=("subtotal", "sum"),
            quantity=("quantity", "sum"),
            orders=("user_id", "count"),
            products=("product_id", "nunique"),
            avg_price=("unit_price", "mean"),
        ).reset_index().sort_values("revenue", ascending=False)

        fig = px.treemap(cat_stats, path=["category"], values="revenue",
                         color="quantity", color_continuous_scale="RdYlGn",
                         title="Category Revenue Treemap (color = quantity)",
                         hover_data=["orders", "products", "avg_price"])
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

    # Revenue vs Quantity scatter
    fig = px.scatter(product_stats, x="revenue", y="quantity", color="category",
                     size="orders", hover_name="product_name",
                     title="Revenue vs Quantity by Product",
                     labels={"revenue": "Revenue (RWF)", "quantity": "Units Sold"},
                     color_discrete_map=cat_color_map)
    fig.update_layout(height=400)
    st.plotly_chart(fig, use_container_width=True)


def page_sessions(sessions_df, sessions_raw):
    st.header("Session & Conversion Analytics")

    # Inline filters
    fc1, fc2 = st.columns(2)
    with fc1:
        all_devices = sorted(sessions_df["device_type"].unique())
        sel_devices = st.multiselect("Device Type", all_devices, default=all_devices)
    with fc2:
        all_referrers = sorted(sessions_df["referrer"].unique())
        sel_referrers = st.multiselect("Referrer", all_referrers, default=all_referrers)
    sessions_df = sessions_df[
        sessions_df["device_type"].isin(sel_devices) & sessions_df["referrer"].isin(sel_referrers)
    ]
    if sessions_df.empty:
        st.warning("No sessions match the selected filters.")
        return
    # Also filter raw sessions
    valid_sids = set(sessions_df["session_id"])
    sessions_raw = [s for s in sessions_raw if s["session_id"] in valid_sids]

    total = len(sessions_df)
    converted = (sessions_df["conversion_status"] == "converted").sum()
    abandoned = (sessions_df["conversion_status"] == "abandoned").sum()
    browsing = (sessions_df["conversion_status"] == "browsing").sum()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Sessions", f"{total:,}")
    c2.metric("Converted", f"{converted:,} ({converted/total*100:.1f}%)")
    c3.metric("Abandoned Cart", f"{abandoned:,} ({abandoned/total*100:.1f}%)")
    c4.metric("Browsing Only", f"{browsing:,} ({browsing/total*100:.1f}%)")

    col1, col2 = st.columns(2)

    with col1:
        # Funnel
        funnel_data = pd.DataFrame({
            "Stage": ["Total Sessions", "Viewed Products", "Added to Cart", "Purchased"],
            "Count": [
                total,
                (sessions_df["viewed_products"] > 0).sum(),
                (sessions_df["cart_items"] > 0).sum(),
                converted,
            ]
        })
        fig = px.funnel(funnel_data, x="Count", y="Stage",
                        title="Conversion Funnel",
                        color_discrete_sequence=[THEME["accent"], THEME["success"], THEME["warning"], THEME["error"]])
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Conversion by device
        device_conv = sessions_df.groupby("device_type").agg(
            total=("session_id", "count"),
            converted=("conversion_status", lambda x: (x == "converted").sum()),
        ).reset_index()
        device_conv["rate"] = device_conv["converted"] / device_conv["total"] * 100
        fig = px.bar(device_conv, x="device_type", y=["converted", "total"],
                     title="Sessions & Conversions by Device",
                     barmode="group",
                     labels={"value": "Count", "device_type": "Device Type"},
                     color_discrete_sequence=[THEME["success"], THEME["accent"]])
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

    col3, col4 = st.columns(2)

    with col3:
        # Session Flow Sankey: referrer -> device -> conversion
        sankey_data = sessions_df.groupby(["referrer", "device_type", "conversion_status"]).size().reset_index(name="count")

        referrers = sorted(sessions_df["referrer"].unique())
        devices = sorted(sessions_df["device_type"].unique())
        statuses = sorted(sessions_df["conversion_status"].unique())

        labels = referrers + [f"{d} " for d in devices] + statuses  # space to avoid collision
        node_colors = ([THEME["accent"]] * len(referrers) +
                       [THEME["warning"]] * len(devices) +
                       [STATUS_COLORS.get(s, THEME["text"]) for s in statuses])

        ref_idx = {r: i for i, r in enumerate(referrers)}
        dev_idx = {d: i + len(referrers) for i, d in enumerate(devices)}
        stat_idx = {s: i + len(referrers) + len(devices) for i, s in enumerate(statuses)}

        sources, targets, values = [], [], []
        # referrer -> device
        rd = sessions_df.groupby(["referrer", "device_type"]).size().reset_index(name="count")
        for _, row in rd.iterrows():
            sources.append(ref_idx[row["referrer"]])
            targets.append(dev_idx[row["device_type"]])
            values.append(row["count"])
        # device -> status
        ds = sessions_df.groupby(["device_type", "conversion_status"]).size().reset_index(name="count")
        for _, row in ds.iterrows():
            sources.append(dev_idx[row["device_type"]])
            targets.append(stat_idx[row["conversion_status"]])
            values.append(row["count"])

        fig = go.Figure(go.Sankey(
            node=dict(pad=15, thickness=20, label=labels, color=node_colors),
            link=dict(source=sources, target=targets, value=values,
                      color="rgba(137,180,250,0.15)"),
        ))
        fig.update_layout(title="Session Flow: Referrer → Device → Outcome", height=400)
        st.plotly_chart(fig, use_container_width=True)
        st.caption("Sankey diagram shows how traffic flows from referrer through device to conversion outcome.")

    with col4:
        # Hourly pattern dual-axis
        hourly = sessions_df.groupby("hour").agg(
            sessions=("session_id", "count"),
            conversions=("conversion_status", lambda x: (x == "converted").sum()),
        ).reset_index()
        hourly["conv_rate"] = hourly["conversions"] / hourly["sessions"] * 100

        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(go.Bar(x=hourly["hour"], y=hourly["sessions"], name="Sessions",
                             marker_color=THEME["accent"], opacity=0.5), secondary_y=False)
        fig.add_trace(go.Scatter(x=hourly["hour"], y=hourly["conv_rate"], name="Conv Rate %",
                                 line=dict(color=THEME["error"], width=2.5), mode="lines+markers"), secondary_y=True)
        fig.update_layout(title="Sessions & Conversion by Hour of Day", height=400)
        fig.update_xaxes(title_text="Hour")
        fig.update_yaxes(title_text="Sessions", secondary_y=False)
        fig.update_yaxes(title_text="Conversion Rate (%)", secondary_y=True)
        st.plotly_chart(fig, use_container_width=True)

    col5, col6 = st.columns(2)

    with col5:
        # Referrer conversion rate
        ref = sessions_df.groupby("referrer").agg(
            sessions=("session_id", "count"),
            conversions=("conversion_status", lambda x: (x == "converted").sum()),
            avg_duration=("duration_seconds", "mean"),
        ).reset_index()
        ref["conv_rate"] = ref["conversions"] / ref["sessions"] * 100
        fig = px.bar(ref, x="referrer", y="conv_rate", color="sessions",
                     title="Conversion Rate by Referrer",
                     labels={"conv_rate": "Conversion Rate (%)", "referrer": "Referrer"},
                     text=ref["conv_rate"].apply(lambda x: f"{x:.1f}%"),
                     color_continuous_scale="Blues")
        fig.update_layout(height=400, coloraxis_showscale=True)
        st.plotly_chart(fig, use_container_width=True)

    with col6:
        # Browser Radar
        browser_stats = sessions_df.groupby("browser").agg(
            sessions=("session_id", "count"),
            conversions=("conversion_status", lambda x: (x == "converted").sum()),
        ).reset_index()
        browser_stats["conv_rate"] = browser_stats["conversions"] / browser_stats["sessions"] * 100
        browsers = browser_stats["browser"].tolist()
        rates = browser_stats["conv_rate"].tolist()

        fig = go.Figure()
        fig.add_trace(go.Scatterpolar(
            r=rates + [rates[0]],
            theta=browsers + [browsers[0]],
            fill="toself",
            fillcolor="rgba(166,227,161,0.2)",
            line=dict(color=THEME["success"], width=2),
            name="Conv Rate %",
        ))
        fig.update_layout(
            title="Conversion Rate by Browser",
            polar=dict(
                bgcolor="rgba(0,0,0,0)",
                radialaxis=dict(gridcolor=THEME["grid"], ticksuffix="%"),
                angularaxis=dict(gridcolor=THEME["grid"]),
            ),
            height=400,
        )
        st.plotly_chart(fig, use_container_width=True)

    # Device/OS heatmap
    st.subheader("Conversion Rate by Device & OS")
    pivot = sessions_df.groupby(["device_type", "device_os"]).agg(
        total=("session_id", "count"),
        converted=("conversion_status", lambda x: (x == "converted").sum()),
    ).reset_index()
    pivot["rate"] = (pivot["converted"] / pivot["total"] * 100).round(1)
    heatmap = pivot.pivot(index="device_type", columns="device_os", values="rate").fillna(0)
    fig = px.imshow(heatmap, text_auto=True, color_continuous_scale="RdYlGn",
                    title="Conversion Rate Heatmap (%)",
                    labels={"x": "OS", "y": "Device Type", "color": "Conv %"})
    fig.update_layout(height=300)
    st.plotly_chart(fig, use_container_width=True)


def page_page_views(sessions_raw):
    st.header("Page View Analytics")

    pv_df = prepare_page_views_df(sessions_raw)

    if pv_df.empty:
        st.warning("No page view data available.")
        return

    # Inline filter: Page Type
    all_page_types = sorted(pv_df["page_type"].unique())
    sel_page_types = st.multiselect("Page Type", all_page_types, default=all_page_types)
    pv_df = pv_df[pv_df["page_type"].isin(sel_page_types)]
    if pv_df.empty:
        st.warning("No page views match the selected filters.")
        return

    # KPIs
    total_views = len(pv_df)
    avg_views_per_session = pv_df.groupby("session_id").size().mean()
    avg_duration = pv_df["view_duration"].mean()
    top_page_type = pv_df["page_type"].mode().iloc[0] if not pv_df["page_type"].mode().empty else "N/A"

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Page Views", f"{total_views:,}")
    c2.metric("Avg Views / Session", f"{avg_views_per_session:.1f}")
    c3.metric("Avg View Duration", f"{avg_duration:.0f}s")
    c4.metric("Top Page Type", top_page_type)

    st.markdown("---")

    col1, col2 = st.columns(2)

    with col1:
        # Page Type Donut
        pt_counts = pv_df["page_type"].value_counts().reset_index()
        pt_counts.columns = ["page_type", "count"]
        fig = px.pie(pt_counts, values="count", names="page_type",
                     title="Views by Page Type", hole=0.45,
                     color_discrete_sequence=CATEGORY_COLORS)
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Avg Duration by Page Type
        dur_by_type = pv_df.groupby("page_type")["view_duration"].mean().sort_values(ascending=True).reset_index()
        dur_by_type.columns = ["page_type", "avg_duration"]
        fig = px.bar(dur_by_type, x="avg_duration", y="page_type", orientation="h",
                     title="Average View Duration by Page Type",
                     labels={"avg_duration": "Avg Duration (s)", "page_type": ""},
                     color="avg_duration", color_continuous_scale="Blues")
        fig.update_layout(height=400, coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True)

    col3, col4 = st.columns(2)

    with col3:
        # Navigation Sankey (page_type[i] -> page_type[i+1])
        seq_df = compute_page_sequences(sessions_raw)
        if not seq_df.empty:
            page_types = sorted(set(seq_df["source"].tolist() + seq_df["target"].tolist()))
            # Prefix target labels to avoid same-name collision
            labels = [f"{pt} (from)" for pt in page_types] + [f"{pt} (to)" for pt in page_types]
            src_idx = {pt: i for i, pt in enumerate(page_types)}
            tgt_idx = {pt: i + len(page_types) for i, pt in enumerate(page_types)}

            sources = [src_idx[r["source"]] for _, r in seq_df.iterrows()]
            targets = [tgt_idx[r["target"]] for _, r in seq_df.iterrows()]
            values = seq_df["count"].tolist()

            colors = (CATEGORY_COLORS * ((len(labels) // len(CATEGORY_COLORS)) + 1))[:len(labels)]

            fig = go.Figure(go.Sankey(
                node=dict(pad=15, thickness=20, label=labels, color=colors),
                link=dict(source=sources, target=targets, value=values,
                          color="rgba(137,180,250,0.12)"),
            ))
            fig.update_layout(title="Page Navigation Flow", height=400)
            st.plotly_chart(fig, use_container_width=True)
            st.caption("Shows the most common page-to-page navigation paths.")

    with col4:
        # Duration Distribution (violin by page type)
        fig = px.violin(pv_df, x="page_type", y="view_duration", color="page_type",
                        title="View Duration Distribution by Page Type",
                        labels={"view_duration": "Duration (s)", "page_type": ""},
                        color_discrete_sequence=CATEGORY_COLORS,
                        box=True, points=False)
        fig.update_layout(height=400, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    # Page Views Over Time (stacked area)
    if "date" in pv_df.columns:
        daily_pv = pv_df.groupby(["date", "page_type"]).size().reset_index(name="views")
        fig = px.area(daily_pv, x="date", y="views", color="page_type",
                      title="Daily Page Views by Type",
                      labels={"date": "Date", "views": "Page Views"},
                      color_discrete_sequence=CATEGORY_COLORS)
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)


def page_cart_orders(txn_df, sessions_raw, prod_info):
    st.header("Cart & Order Analytics")

    # Inline filter: Order Status
    if "status" in txn_df.columns:
        all_statuses = sorted(txn_df["status"].unique())
        sel_statuses = st.multiselect("Order Status", all_statuses, default=all_statuses)
        txn_df = txn_df[txn_df["status"].isin(sel_statuses)]

    cart_df = prepare_cart_df(sessions_raw, prod_info)

    # KPIs
    if not cart_df.empty:
        cart_totals = cart_df.groupby("session_id")["line_total"].sum()
        cart_items_count = cart_df.groupby("session_id")["quantity"].sum()
        avg_cart_value = cart_totals.mean()
        avg_cart_items = cart_items_count.mean()
    else:
        avg_cart_value = 0
        avg_cart_items = 0

    total_txn = txn_df["transaction_id"].nunique() if "transaction_id" in txn_df.columns else 0
    refunded = txn_df[txn_df["status"] == "refunded"]["transaction_id"].nunique() if "transaction_id" in txn_df.columns else 0
    refund_rate = refunded / total_txn * 100 if total_txn > 0 else 0

    sessions_with_cart = (cart_df["session_id"].nunique()) if not cart_df.empty else 0
    converted_carts = cart_df[cart_df["conversion_status"] == "converted"]["session_id"].nunique() if not cart_df.empty else 0
    abandonment_rate = (1 - converted_carts / sessions_with_cart) * 100 if sessions_with_cart > 0 else 0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Avg Cart Value", f"RWF {avg_cart_value:,.0f}")
    c2.metric("Avg Cart Items", f"{avg_cart_items:.1f}")
    c3.metric("Cart Abandonment", f"{abandonment_rate:.1f}%")
    c4.metric("Refund Rate", f"{refund_rate:.1f}%")

    st.markdown("---")

    col1, col2 = st.columns(2)

    with col1:
        # Status Gauges
        if "status" in txn_df.columns and "transaction_id" in txn_df.columns:
            status_counts = txn_df.groupby("status")["transaction_id"].nunique()
            total_orders = status_counts.sum()
            statuses = ["completed", "shipped", "processing", "refunded"]
            gauge_colors = [STATUS_COLORS.get(s, THEME["accent"]) for s in statuses]

            fig = make_subplots(rows=1, cols=4,
                                specs=[[{"type": "indicator"}] * 4])
            for i, status in enumerate(statuses):
                count = status_counts.get(status, 0)
                pct = count / total_orders * 100 if total_orders > 0 else 0
                fig.add_trace(go.Indicator(
                    mode="gauge+number",
                    value=pct,
                    title={"text": status.title(), "font": {"size": 12}},
                    number={"suffix": "%", "font": {"size": 18}},
                    gauge={
                        "axis": {"range": [0, 100]},
                        "bar": {"color": gauge_colors[i]},
                        "bgcolor": "rgba(0,0,0,0)",
                    },
                ), row=1, col=i + 1)
            fig.update_layout(height=250, margin=dict(t=40, b=10))
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Cart Value Distribution
        if not cart_df.empty:
            cart_session_totals = cart_df.groupby("session_id")["line_total"].sum().reset_index()
            cart_session_totals.columns = ["session_id", "cart_total"]
            fig = px.histogram(cart_session_totals, x="cart_total",
                               title="Cart Value Distribution",
                               labels={"cart_total": "Cart Total (RWF)", "count": "Sessions"},
                               nbins=40, marginal="box",
                               color_discrete_sequence=[THEME["accent"]])
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

    col3, col4 = st.columns(2)

    with col3:
        # Discount Waterfall: Gross -> Discount -> Net
        if "transaction_id" in txn_df.columns:
            txn_agg = txn_df.groupby("transaction_id").agg(
                subtotal=("subtotal", "sum"),
                discount=("discount", "first"),
                total=("total", "first"),
            ).reset_index()
            gross = txn_agg["subtotal"].sum()
            disc = txn_agg["discount"].sum()
            net = txn_agg["total"].sum()

            fig = go.Figure(go.Waterfall(
                name="Revenue Flow",
                orientation="v",
                measure=["absolute", "relative", "total"],
                x=["Gross Revenue", "Discounts", "Net Revenue"],
                y=[gross, -disc, 0],
                connector={"line": {"color": THEME["grid"]}},
                increasing={"marker": {"color": THEME["success"]}},
                decreasing={"marker": {"color": THEME["error"]}},
                totals={"marker": {"color": THEME["accent"]}},
                text=[f"RWF {gross:,.0f}", f"-RWF {disc:,.0f}", f"RWF {net:,.0f}"],
                textposition="outside",
            ))
            fig.update_layout(title="Revenue Waterfall: Gross → Discount → Net", height=400)
            st.plotly_chart(fig, use_container_width=True)
            st.caption(f"Total discounts account for {disc/gross*100:.1f}% of gross revenue." if gross > 0 else "")

    with col4:
        # Refund by Category
        if "status" in txn_df.columns:
            cat_status = txn_df.groupby(["category", "status"]).agg(
                count=("transaction_id", "nunique") if "transaction_id" in txn_df.columns else ("user_id", "count"),
            ).reset_index()
            cat_total = cat_status.groupby("category")["count"].sum().reset_index()
            cat_total.columns = ["category", "total"]
            cat_ref = cat_status[cat_status["status"] == "refunded"].copy()
            cat_ref = cat_ref.merge(cat_total, on="category")
            cat_ref["refund_rate"] = cat_ref["count"] / cat_ref["total"] * 100

            fig = px.bar(cat_ref.sort_values("refund_rate", ascending=True),
                         x="refund_rate", y="category", orientation="h",
                         title="Refund Rate by Category",
                         labels={"refund_rate": "Refund Rate (%)", "category": ""},
                         color="refund_rate", color_continuous_scale="Reds",
                         text=cat_ref.sort_values("refund_rate", ascending=True)["refund_rate"].apply(lambda x: f"{x:.1f}%"))
            fig.update_layout(height=400, coloraxis_showscale=False)
            st.plotly_chart(fig, use_container_width=True)

    # Abandonment by Referrer (stacked by device)
    if not cart_df.empty:
        # Merge session info with cart data
        sessions_df_mini = pd.DataFrame([
            {"session_id": s["session_id"], "referrer": s["referrer"],
             "device_type": s["device_profile"]["type"]}
            for s in sessions_raw
        ])
        abandon_df = cart_df[["session_id", "conversion_status"]].drop_duplicates()
        abandon_df = abandon_df.merge(sessions_df_mini, on="session_id", how="left")
        abandon_df["abandoned"] = (abandon_df["conversion_status"] != "converted").astype(int)

        aband_ref = abandon_df.groupby(["referrer", "device_type"])["abandoned"].sum().reset_index()
        fig = px.bar(aband_ref, x="referrer", y="abandoned", color="device_type",
                     title="Cart Abandonment by Referrer & Device",
                     labels={"abandoned": "Abandoned Carts", "referrer": "Referrer"},
                     barmode="stack", color_discrete_sequence=CATEGORY_COLORS)
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)


# ---------------------------------------------------------------------------
# Spark Analytics Page
# ---------------------------------------------------------------------------

def _spark_available():
    """Check if PySpark is importable."""
    try:
        import pyspark  # noqa: F401
        return True
    except ImportError:
        return False


@st.cache_resource
def _get_spark():
    """Get or create a SparkSession (persisted across reruns)."""
    import sys
    from pyspark.sql import SparkSession
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    return (
        SparkSession.builder
        .appName("DashboardSpark")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


@st.cache_data(ttl=300, show_spinner="Running Spark analytics...")
def _run_spark_analytics():
    """Run all Spark analytics and cache results for 5 minutes."""
    import sys
    sys.path.insert(0, os.path.dirname(__file__))
    from spark_processing import load_and_clean, product_recommendations, cohort_analysis, spark_sql_queries
    from analytics_integration import analysis_clv, analysis_funnel

    spark = _get_spark()
    spark.sparkContext.setLogLevel("WARN")

    dfs = load_and_clean(spark)
    recs = product_recommendations(dfs)
    cohort_spend, cohort_retention = cohort_analysis(dfs)
    sql_results = spark_sql_queries(dfs)
    clv_results = analysis_clv(spark)
    funnel_results = analysis_funnel(spark)

    return {
        "recs": recs,
        "cohort_spend": cohort_spend,
        "cohort_retention": cohort_retention,
        **sql_results,
        **clv_results,
        **funnel_results,
    }


def page_spark():
    """Spark Analytics page — runs Spark jobs on-demand and displays results."""
    st.header("Spark Analytics")

    if not _spark_available():
        st.error("Spark is not available. Ensure PySpark and Java are installed.")
        st.info("Install: `uv pip install pyspark`")
        return

    try:
        data = _run_spark_analytics()
    except Exception as e:
        st.error(f"Spark analytics failed: {e}")
        st.info("Ensure PySpark and Java are installed and data files exist in `data/`.")
        return

    # ── Section 1: Customer Lifetime Value ──
    st.subheader("Customer Lifetime Value")

    col1, col2 = st.columns(2)

    with col1:
        tier_df = data["tier_summary"]
        fig = px.bar(
            tier_df, x="clv_tier", y="num_customers", color="avg_clv",
            title="CLV Tier Distribution",
            labels={"clv_tier": "Tier", "num_customers": "Customers", "avg_clv": "Avg CLV"},
            color_continuous_scale="Viridis",
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        prov_df = data["province_summary"]
        fig = px.bar(
            prov_df.head(15), x="avg_clv", y="province", orientation="h",
            title="Average CLV by Province (Top 15)",
            labels={"avg_clv": "Avg CLV", "province": "Province"},
            color="total_revenue", color_continuous_scale="Viridis",
        )
        fig.update_layout(height=400, yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("**Top 15 Customers by Estimated CLV**")
    clv_display = data["clv"].head(15)
    display_cols = [c for c in ["user_id", "name", "province", "estimated_clv", "total_spent",
                                "total_orders", "session_count"] if c in clv_display.columns]
    st.dataframe(clv_display[display_cols], use_container_width=True, hide_index=True)

    st.markdown("---")

    # ── Section 2: Product Intelligence ──
    st.subheader("Product Intelligence")

    col1, col2 = st.columns(2)

    with col1:
        top_prod = data["top_products"]
        fig = px.bar(
            top_prod, x="total_revenue", y="product_name", orientation="h",
            title="Top 15 Products by Revenue (with Est. Profit)",
            labels={"total_revenue": "Revenue", "product_name": "Product"},
            color="est_profit", color_continuous_scale="Viridis",
        )
        fig.update_layout(height=500, yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        elast_df = data["price_elasticity"]
        if not elast_df.empty and "price_variability_ratio" in elast_df.columns:
            plot_df = elast_df.dropna(subset=["price_variability_ratio"])
            if not plot_df.empty:
                fig = px.scatter(
                    plot_df, x="price_variability_ratio", y="mean_daily_units",
                    size="total_units", hover_name="product_name",
                    title="Price Elasticity: Variability Ratio vs Daily Sales",
                    labels={"price_variability_ratio": "Price Variability Ratio",
                            "mean_daily_units": "Mean Daily Units"},
                )
                fig.update_layout(height=500)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No price elasticity data available.")
        else:
            st.info("No price elasticity data available.")

    st.markdown("**Co-Purchase Recommendations**")
    recs_df = data["recs"]
    search = st.text_input("Search product", "", key="spark_rec_search")
    if search:
        mask = (recs_df["source_name"].str.contains(search, case=False, na=False) |
                recs_df["rec_name"].str.contains(search, case=False, na=False))
        recs_df = recs_df[mask]
    st.dataframe(recs_df, use_container_width=True, hide_index=True, height=400)

    st.markdown("---")

    # ── Section 3: Funnel & Device Deep-Dive ──
    st.subheader("Funnel & Device Deep-Dive")

    col1, col2 = st.columns(2)

    with col1:
        fd = data["funnel_device"]
        rate_cols = [c for c in ["view_rate_pct", "cart_rate_pct", "purchase_rate_pct"] if c in fd.columns]
        if rate_cols:
            fd_melted = fd.melt(id_vars="device_type", value_vars=rate_cols,
                                var_name="stage", value_name="rate")
            fig = px.bar(
                fd_melted, x="device_type", y="rate", color="stage", barmode="group",
                title="Funnel Rates by Device Type",
                labels={"device_type": "Device", "rate": "Rate (%)", "stage": "Stage"},
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        fr = data["funnel_referrer"]
        rate_cols = [c for c in ["view_rate_pct", "cart_rate_pct", "purchase_rate_pct"] if c in fr.columns]
        if rate_cols:
            fr_melted = fr.melt(id_vars="referrer", value_vars=rate_cols,
                                var_name="stage", value_name="rate")
            fig = px.bar(
                fr_melted, x="referrer", y="rate", color="stage", barmode="group",
                title="Funnel Rates by Referrer",
                labels={"referrer": "Referrer", "rate": "Rate (%)", "stage": "Stage"},
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

    col1, col2 = st.columns(2)

    with col1:
        ca = data["cart_abandonment"]
        fig = px.bar(
            ca, x="device_type", y="abandonment_rate_pct",
            title="Cart Abandonment Rate by Device",
            labels={"device_type": "Device", "abandonment_rate_pct": "Abandonment Rate (%)"},
            color="abandonment_rate_pct", color_continuous_scale="Reds",
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        ret_df = data["cohort_retention"]
        if not ret_df.empty and "cohort" in ret_df.columns:
            # Build heatmap from cohort retention data
            heatmap_df = ret_df.set_index("cohort")
            # Drop cohort_size if present
            heatmap_df = heatmap_df.drop(columns=["cohort_size"], errors="ignore")
            # Only keep numeric columns
            heatmap_df = heatmap_df.select_dtypes(include="number")
            if not heatmap_df.empty:
                fig = px.imshow(
                    heatmap_df, aspect="auto",
                    title="Cohort Retention Heatmap (% of cohort)",
                    labels=dict(x="Months Since Registration", y="Cohort", color="Retention %"),
                    color_continuous_scale="YlGnBu",
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No retention data to display.")
        else:
            st.info("No cohort retention data available.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    inject_custom_css()

    products, users, transactions, sessions, categories, source, session_source = load_data()
    prod_info = build_product_map(products, categories)

    txn_df = prepare_transactions_df(transactions, prod_info)
    users_df = prepare_users_df(users)
    sessions_df = prepare_sessions_df(sessions)

    # Sidebar
    st.sidebar.title("E-Commerce Analytics")
    st.sidebar.caption(f"Products/Users/Transactions: **{source}**")
    st.sidebar.caption(f"Sessions: **{session_source}**")
    st.sidebar.caption(f"Spark: **{'Available' if _spark_available() else 'Not configured'}**")
    st.sidebar.markdown("---")

    page = st.sidebar.radio("Navigate", [
        "Overview",
        "Customers",
        "Products",
        "Sessions & Conversion",
        "Page View Analytics",
        "Cart & Order Analytics",
        "Spark Analytics",
    ])

    # --- Global Filters ---
    st.sidebar.markdown("---")
    st.sidebar.markdown("**Filters**")

    min_date = txn_df["date"].min().date()
    max_date = txn_df["date"].max().date()
    date_range = st.sidebar.date_input(
        "Date Range", value=(min_date, max_date),
        min_value=min_date, max_value=max_date,
    )

    all_categories = sorted(txn_df["category"].unique())
    selected_categories = st.sidebar.multiselect("Category", all_categories, default=all_categories)

    all_provinces = sorted(users_df["province"].unique())
    selected_provinces = st.sidebar.multiselect("Province", all_provinces, default=all_provinces)

    # Apply global filters
    if len(date_range) == 2:
        d_start = pd.Timestamp(date_range[0])
        d_end = pd.Timestamp(date_range[1])
        txn_df = txn_df[(txn_df["date"] >= d_start) & (txn_df["date"] <= d_end)]
        # Match timezone of start_time column for comparison
        st_col = sessions_df["start_time"]
        if hasattr(st_col.dtype, "tz") and st_col.dtype.tz is not None:
            d_start_tz = d_start.tz_localize(st_col.dtype.tz)
            d_end_tz = (d_end + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)).tz_localize(st_col.dtype.tz)
        else:
            d_start_tz = d_start
            d_end_tz = d_end + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
        sessions_df = sessions_df[
            (sessions_df["start_time"] >= d_start_tz) & (sessions_df["start_time"] <= d_end_tz)
        ]

    if selected_categories and len(selected_categories) < len(all_categories):
        txn_df = txn_df[txn_df["category"].isin(selected_categories)]

    if selected_provinces and len(selected_provinces) < len(all_provinces):
        users_df = users_df[users_df["province"].isin(selected_provinces)]
        sessions_df = sessions_df[sessions_df["province"].isin(selected_provinces)]

    # Filter raw sessions to match sessions_df (for page_views / cart pages)
    valid_sids = set(sessions_df["session_id"])
    sessions_filtered = [s for s in sessions if s["session_id"] in valid_sids]

    st.sidebar.markdown("---")
    st.sidebar.markdown("**Dataset Summary**")
    st.sidebar.markdown(f"- {len(users):,} users ({len(users_df):,} filtered)")
    st.sidebar.markdown(f"- {len(products):,} products")
    st.sidebar.markdown(f"- {len(transactions):,} transactions ({len(txn_df):,} rows filtered)")
    st.sidebar.markdown(f"- {len(sessions):,} sessions ({len(sessions_filtered):,} filtered)")
    st.sidebar.markdown(f"- {len(categories)} categories")

    if page == "Overview":
        page_overview(txn_df, users_df, sessions_df, transactions)
    elif page == "Customers":
        page_customers(users_df, txn_df, sessions_df)
    elif page == "Products":
        page_products(txn_df, prod_info, products, categories)
    elif page == "Sessions & Conversion":
        page_sessions(sessions_df, sessions_filtered)
    elif page == "Page View Analytics":
        page_page_views(sessions_filtered)
    elif page == "Cart & Order Analytics":
        page_cart_orders(txn_df, sessions_filtered, prod_info)
    elif page == "Spark Analytics":
        page_spark()


if __name__ == "__main__":
    main()
