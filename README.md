# E-Commerce Analytics Platform

Advanced Database Design and Implementation — ULK MSE Final Project.

A multi-database analytics system built on synthetic e-commerce data using MongoDB, HBase, and Apache Spark.

## Architecture

| Component | Role | Access |
|-----------|------|--------|
| **MongoDB** | Document store — product catalog, user profiles, transactions | `localhost:27017` |
| **HBase** | Wide-column store — time-series sessions, product metrics | `localhost:9090` (Thrift) |
| **Spark** | Batch processing — data pipelines, cross-system analytics | `localhost:7077` |

## Dataset

Generated synthetic e-commerce data (Dec 2025 -- Mar 2026, 90 days):
- 2,000 users, 800 products, 15 categories
- 20,000 browsing sessions, ~4,400 transactions

## Quick Start

### Option A: WSL / x86_64 (recommended)

Auto-generates data and seeds databases on startup:

```bash
docker compose -f docker-compose.yml -f docker-compose.wsl.yml up -d

# Watch the init container generate data + seed databases
docker logs -f ecommerce-init

# Verify all services are healthy (init should exit 0)
docker compose -f docker-compose.yml -f docker-compose.wsl.yml ps
```

### Option B: Apple Silicon / manual setup

HBase runs under x86 emulation (slower startup, ~5-10 min):

```bash
# 1. Install Python dependencies
uv sync

# 2. Start infrastructure
docker compose up -d

# 3. Generate dataset and seed databases
source .venv/bin/activate
python src/dataset_generator.py
python src/seed_databases.py

# 4. Verify services
docker compose ps
```

### Dashboard

Once services are running and data is seeded, the dashboard is available at http://localhost:8501.

A live version is hosted at https://dbms-fat.bahatijustin.dev.

## Project Structure

```
├── src/                      # Python source code
├── data/                     # Generated datasets (gitignored)
├── docs/                     # Project PDFs and instructions
├── docker-compose.yml        # Base: MongoDB, HBase, ZooKeeper, Spark, Dashboard
├── docker-compose.wsl.yml    # WSL override: native HBase + init container
├── Dockerfile.dashboard      # Dashboard image (Streamlit + PySpark)
├── Dockerfile.init           # Init image (data generation + seeding)
└── pyproject.toml             # Python dependencies (uv)
```

## Services

| Service | Image | Ports |
|---------|-------|-------|
| MongoDB | `mongo:7.0` | 27017 |
| ZooKeeper | `zookeeper:3.9` | 2181 |
| HBase | `dajobe/hbase` | 9090, 16010 (UI) |
| Spark Master | `apache/spark:4.0.2-python3` | 7077, 8080 (UI) |
| Spark Worker | `apache/spark:4.0.2-python3` | 8081 (UI) |
| Dashboard | `Dockerfile.dashboard` | 8501 |
| Init (WSL only) | `Dockerfile.init` | — (runs once, exits) |

## Requirements

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) for package management
- Docker & Docker Compose
