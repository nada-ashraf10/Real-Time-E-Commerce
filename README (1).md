# 🛒 Real-Time E-Commerce: End-to-End Sales Analytics Platform
### Azure Event Hubs · Databricks Medallion Architecture · Delta Lake · Power BI

---

## 📌 Overview

**Real-Time E-Commerce** is a production-grade, end-to-end data engineering and analytics platform built on the [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) from Kaggle.

The project covers the complete data lifecycle — from raw ingestion through real-time streaming to business intelligence — using Azure and Databricks cloud-native services.

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                             │
│   Kaggle CSV Files (Olist Dataset)  +  Azure Event Hubs Stream  │
└───────────────┬──────────────────────────────┬──────────────────┘
                │                              │
                ▼                              ▼
┌──────────────────────────┐    ┌─────────────────────────────────┐
│    BRONZE LAYER           │    │    REAL-TIME STREAMING          │
│  Raw ingestion → Delta    │    │  Producer → Event Hub           │
│  9 tables, schema infer   │    │  Consumer → bronze Delta table  │
└────────────┬─────────────┘    └─────────────────────────────────┘
             │
             ▼
┌──────────────────────────┐
│    SILVER LAYER           │
│  Cleaned & validated      │
│  Type casting, dedup,     │
│  null handling, enrichment│
└────────────┬─────────────┘
             │
             ▼
┌──────────────────────────┐
│    GOLD LAYER             │
│  Star schema (Dim + Fact) │
│  Business-ready metrics   │
└────────────┬─────────────┘
             │
             ▼
┌──────────────────────────┐
│    POWER BI DASHBOARD     │
│  KPIs, trends, geo maps   │
│  Delivery & payment intel │
└──────────────────────────┘
```

---

## 📂 Project Structure

```
real-time-ecommerce/
│
├── etl_pipeline/
│   ├── bronze_layer.py          # Raw CSV → Bronze Delta tables
│   ├── silver_layer.py          # Cleaned & validated Silver tables
│   └── gold_layer.py            # Star schema Gold layer (Dims + Facts)
│
├── streaming/
│   ├── Producer.py              # Azure Event Hubs producer (order payments)
│   └── Consumer.py              # Event Hubs consumer → Bronze Delta table
│
├── dashboard/
│   └── Olist_PowerBI_Dashboard.pbix   # Power BI report file
│
└── README.md
```

---

## 🥉 Bronze Layer — Raw Ingestion

Reads 9 CSV files from a Databricks Volume and writes them as Delta tables into `bronze_schema`.

| Table | Description |
|---|---|
| `customers` | Customer demographics |
| `geolocation` | ZIP-level lat/lng data |
| `order_items` | Line items per order |
| `order_payments` | Payment transactions |
| `order_reviews` | Customer review scores & comments |
| `orders` | Order lifecycle timestamps |
| `products` | Product metadata |
| `sellers` | Seller location data |
| `product_category_translation` | PT → EN category names |

---

## 🥈 Silver Layer — Cleaning & Validation

Applies per-table transformations:

- **Deduplication** using window functions and `dropDuplicates`
- **Type casting** (timestamps, doubles, integers)
- **Null handling** — filtering on critical keys, filling defaults
- **String normalization** — `trim`, `lower`, `upper`, `initcap`, `lpad`
- **Derived columns** — `delivery_delay_days`, `review_sentiment`
- **Data quality checks** — validates review scores (1–5), payment values (> 0), state codes (length == 2)

---

## 🥇 Gold Layer — Star Schema

Business-ready dimensional model written to `gold_schema`.

**Dimension Tables**

| Table | Key |
|---|---|
| `dim_customers` | `customer_id` |
| `dim_sellers` | `seller_id` |
| `dim_products` | `product_id` (joined with EN category names) |
| `dim_date` | `date_id` (yyyyMMdd integer key) |
| `dim_reviews` | `review_id` (with sentiment classification) |
| `dim_geolocation` | `geolocation_zip_code_prefix` |

**Fact Tables**

| Table | Grain |
|---|---|
| `fact_order_items` | One row per order line item |
| `fact_payments` | One row per payment transaction |
| `fact_order_lifecycle` | One row per order (aggregated metrics) |

---

## ⚡ Real-Time Streaming (Azure Event Hubs)

Simulates live payment data flowing into the platform.

- **Producer** (`Producer.py`) — reads the payments CSV and sends each row as a JSON event to Azure Event Hubs with a 1-second delay between events.
- **Consumer** (`Consumer.py`) — listens on the Event Hub, deserializes each event, creates a Spark DataFrame, and appends it to the Bronze Delta table `stream_order_payments`.

**Tech:** `azure-eventhub` SDK · Databricks notebooks · Delta Lake append mode with schema merging

---

## 📊 Power BI Dashboard

Connected to the Gold layer via Databricks connector. Key visuals include:

- **Revenue trends** — monthly and quarterly GMV
- **Delivery performance** — on-time vs. late orders, average delay days
- **Payment method breakdown** — credit card, boleto, voucher, debit
- **Review sentiment analysis** — positive / neutral / negative by category
- **Geo distribution** — orders and revenue by Brazilian state
- **Top product categories** — by revenue and order volume

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Cloud Platform | Microsoft Azure |
| Compute | Azure Databricks |
| Storage Format | Delta Lake |
| Streaming | Azure Event Hubs |
| Query Engine | Apache Spark (PySpark + Spark SQL) |
| BI & Visualization | Microsoft Power BI |
| Dataset | Olist Brazilian E-Commerce (Kaggle) |

---

## 🚀 How to Run

### Prerequisites
- Azure Databricks workspace with Unity Catalog enabled
- Azure Event Hubs namespace (`sales-streaming-2026`)
- Catalog: `sales_databricks` with schemas `bronze_schema`, `silver_schema`, `gold_schema`
- Olist CSV files uploaded to `/Volumes/sales_databricks/bronze_schema/raw_files/`

### Execution Order

```
1. etl_pipeline/bronze_layer.py     ← Load raw CSVs into Delta
2. etl_pipeline/silver_layer.py     ← Clean and validate
3. etl_pipeline/gold_layer.py       ← Build star schema
4. streaming/Producer.py            ← Start payment event stream
5. streaming/Consumer.py            ← Consume events into Delta  (run in parallel with Producer)
6. Open Olist_PowerBI_Dashboard.pbix in Power BI Desktop
```

---

## 📈 Business Questions Answered

1. What is the monthly and quarterly revenue trend?
2. Which product categories generate the most revenue?
3. What percentage of orders are delivered late, and by how many days on average?
4. How do customers pay — and what is the average installment count?
5. Which Brazilian states have the most orders and highest GMV?
6. How does review sentiment correlate with delivery performance?

---

## 📄 Dataset

[Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) — Kaggle

> ~100K orders from 2016–2018 across multiple marketplaces in Brazil, covering orders, payments, reviews, products, sellers, and customers.

---

## 👤 Author

Built as a final capstone project demonstrating end-to-end data engineering skills:
**ETL pipelines · Streaming ingestion · Dimensional modeling · Business intelligence**
