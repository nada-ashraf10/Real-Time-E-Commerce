# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS sales_databricks.gold_schema;
# MAGIC USE sales_databricks.gold_schema;
# MAGIC
# MAGIC --dims
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS dim_customers (
# MAGIC     customer_id                STRING,
# MAGIC     customer_unique_id         STRING,
# MAGIC     customer_zip_code_prefix   INT,
# MAGIC     customer_city              STRING,
# MAGIC     customer_state             STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS dim_sellers (
# MAGIC     seller_id                  STRING,
# MAGIC     seller_zip_code_prefix     INT,
# MAGIC     seller_city                STRING,
# MAGIC     seller_state               STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS dim_products (
# MAGIC     product_id                       STRING,
# MAGIC     product_category_name            STRING,
# MAGIC     product_category_name_english    STRING,
# MAGIC     product_photos_qty               INT,
# MAGIC     product_weight_g                 DOUBLE,
# MAGIC     product_length_cm                DOUBLE,
# MAGIC     product_height_cm                DOUBLE,
# MAGIC     product_width_cm                 DOUBLE,
# MAGIC     product_volume_cm3               DOUBLE
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS dim_date (
# MAGIC     date_id        INT,
# MAGIC     full_date      DATE,
# MAGIC     year           INT,
# MAGIC     quarter        INT,
# MAGIC     month          INT,
# MAGIC     month_name     STRING,
# MAGIC     day            INT,
# MAGIC     day_of_week    INT,
# MAGIC     day_name       STRING,
# MAGIC     is_weekend     BOOLEAN
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS dim_reviews (
# MAGIC     review_id                  STRING,
# MAGIC     order_id                   STRING,
# MAGIC     review_score               INT,
# MAGIC     review_sentiment           STRING,
# MAGIC     review_comment_message     STRING,
# MAGIC     has_comment                BOOLEAN,
# MAGIC     review_creation_date       TIMESTAMP,
# MAGIC     review_answer_timestamp    TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS dim_geolocation (
# MAGIC     geolocation_zip_code_prefix    INT,
# MAGIC     geolocation_city               STRING,
# MAGIC     geolocation_state              STRING,
# MAGIC     latitude                       DOUBLE,
# MAGIC     longitude                      DOUBLE
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC --fact tables
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS fact_order_items (
# MAGIC     order_id          STRING,
# MAGIC     product_id        STRING,
# MAGIC     seller_id         STRING,
# MAGIC     customer_id       STRING,
# MAGIC     date_id           INT,
# MAGIC     order_item_id     INT,
# MAGIC     price             DOUBLE,
# MAGIC     freight_value     DOUBLE,
# MAGIC     total_item_value  DOUBLE
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS fact_payments (
# MAGIC     payment_key            STRING,
# MAGIC     order_id               STRING,
# MAGIC     payment_sequential     INT,
# MAGIC     payment_type           STRING,
# MAGIC     payment_installments   INT,
# MAGIC     payment_value          DOUBLE
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS fact_order_lifecycle (
# MAGIC     order_id                         STRING,
# MAGIC     customer_id                      STRING,
# MAGIC     review_id                        STRING,
# MAGIC     date_id                          INT,
# MAGIC     order_status                     STRING,
# MAGIC     order_purchase_timestamp         TIMESTAMP,
# MAGIC     order_approved_at                TIMESTAMP,
# MAGIC     order_delivered_carrier_date     TIMESTAMP,
# MAGIC     order_delivered_customer_date    TIMESTAMP,
# MAGIC     order_estimated_delivery_date    TIMESTAMP,
# MAGIC     delivery_delay_days              INT,
# MAGIC     total_payment_value              DOUBLE,
# MAGIC     payment_methods_count            INT,
# MAGIC     review_score                     INT,
# MAGIC     is_late                          BOOLEAN,
# MAGIC     is_delivered                     BOOLEAN,
# MAGIC     total_items_price                DOUBLE,
# MAGIC     total_freight_value              DOUBLE,
# MAGIC     total_order_value                DOUBLE,
# MAGIC     total_items_count                INT
# MAGIC ) USING DELTA;

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType

# ============================================================
# READ ALL SILVER TABLES
# ============================================================
customers_silver      = spark.read.table("sales_databricks.silver_schema.customers")
sellers_silver        = spark.read.table("sales_databricks.silver_schema.sellers")
products_silver       = spark.read.table("sales_databricks.silver_schema.products")
orders_silver         = spark.read.table("sales_databricks.silver_schema.orders")
order_items_silver    = spark.read.table("sales_databricks.silver_schema.order_items")
order_payments_silver = spark.read.table("sales_databricks.silver_schema.order_payments")
order_reviews_silver  = spark.read.table("sales_databricks.silver_schema.order_reviews")
geolocation_silver    = spark.read.table("sales_databricks.silver_schema.geolocation")

# ============================================================
# DIM_CUSTOMERS
# ============================================================
dim_customers = customers_silver.select(
    "customer_id",
    "customer_unique_id",
    "customer_zip_code_prefix",
    "customer_city",
    "customer_state"
).dropDuplicates(["customer_id"])

dim_customers.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("sales_databricks.gold_schema.dim_customers")
print(f"✓ dim_customers: {dim_customers.count()} rows")

# ============================================================
# DIM_SELLERS
# ============================================================
dim_sellers = sellers_silver.select(
    "seller_id",
    "seller_zip_code_prefix",
    "seller_city",
    "seller_state"
).dropDuplicates(["seller_id"])

dim_sellers.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("sales_databricks.gold_schema.dim_sellers")
print(f"✓ dim_sellers: {dim_sellers.count()} rows")

# ============================================================
# DIM_PRODUCTS
# ============================================================
# ============================================================
# DIM_PRODUCTS — join translation here since it wasn't done in Silver
# ============================================================
translation_silver = spark.read.table("sales_databricks.silver_schema.product_category_translation") \
    .select("product_category_name", "product_category_name_english")

# ============================================================
# DIM_PRODUCTS
# ============================================================


dim_products = products_silver \
    .join(translation_silver, on="product_category_name", how="left") \
    .select(
        products_silver["product_id"],
        products_silver["product_category_name"],
        "product_category_name_english",
        F.col("product_photos_qty").cast(IntegerType()),
        F.col("product_weight_g").cast(DoubleType()),
        F.col("product_length_cm").cast(DoubleType()),
        F.col("product_height_cm").cast(DoubleType()),
        F.col("product_width_cm").cast(DoubleType()),
    ) \
    .withColumn("product_volume_cm3",
        F.col("product_length_cm") *
        F.col("product_height_cm") *
        F.col("product_width_cm")
    ) \
    .withColumn("product_category_name_english",
        F.when(F.col("product_category_name_english").isNull(), "unknown")
         .otherwise(F.col("product_category_name_english"))
    ) \
    .dropDuplicates(["product_id"])

dim_products.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("sales_databricks.gold_schema.dim_products")
print(f"✓ dim_products: {dim_products.count()} rows")

# ============================================================
# DIM_DATE
# ============================================================
dim_date = orders_silver \
    .select(F.to_date("order_purchase_timestamp").alias("full_date")) \
    .filter(F.col("full_date").isNotNull()) \
    .dropDuplicates(["full_date"]) \
    .withColumn("date_id",     F.date_format("full_date", "yyyyMMdd").cast(IntegerType())) \
    .withColumn("year",        F.year("full_date")) \
    .withColumn("quarter",     F.quarter("full_date")) \
    .withColumn("month",       F.month("full_date")) \
    .withColumn("month_name",  F.date_format("full_date", "MMMM")) \
    .withColumn("day",         F.dayofmonth("full_date")) \
    .withColumn("day_of_week", F.dayofweek("full_date")) \
    .withColumn("day_name",    F.date_format("full_date", "EEEE")) \
    .withColumn("is_weekend",  F.dayofweek("full_date").isin([1, 7])) \
    .select("date_id", "full_date", "year", "quarter", "month",
            "month_name", "day", "day_of_week", "day_name", "is_weekend") \
    .orderBy("date_id")

dim_date.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("sales_databricks.gold_schema.dim_date")
print(f"✓ dim_date: {dim_date.count()} rows")

# ============================================================
# DIM_REVIEWS
# ============================================================
dim_reviews = order_reviews_silver.select(
    "review_id",
    "order_id",
    F.col("review_score").cast(IntegerType()),
    "review_comment_message",
    "review_creation_date",
    "review_answer_timestamp",
).withColumn("review_sentiment",
    F.when(F.col("review_score") >= 4, "Positive")
     .when(F.col("review_score") == 3, "Neutral")
     .otherwise("Negative")
).withColumn("has_comment",
    F.when(F.col("review_comment_message") == "no comment", False)
     .otherwise(True)
).dropDuplicates(["review_id"])

dim_reviews.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("sales_databricks.gold_schema.dim_reviews")
print(f"✓ dim_reviews: {dim_reviews.count()} rows")

# ============================================================
# DIM_GEOLOCATION
# ============================================================
dim_geolocation = geolocation_silver \
    .groupBy("geolocation_zip_code_prefix", "geolocation_city", "geolocation_state") \
    .agg(
        F.avg("geolocation_lat").alias("latitude"),
        F.avg("geolocation_lng").alias("longitude"),
    ) \
    .dropDuplicates(["geolocation_zip_code_prefix"])

dim_geolocation.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("sales_databricks.gold_schema.dim_geolocation")
print(f"✓ dim_geolocation: {dim_geolocation.count()} rows")

# ============================================================
# FACT_ORDER_ITEMS
# ============================================================
fact_order_items = order_items_silver \
    .join(
        orders_silver.select("order_id", "order_purchase_timestamp", "customer_id"),
        on="order_id", how="inner"
    ).select(
        order_items_silver["order_id"],
        "product_id",
        "seller_id",
        "customer_id",
        F.date_format("order_purchase_timestamp", "yyyyMMdd").cast(IntegerType()).alias("date_id"),
        F.col("order_item_id").cast(IntegerType()),
        F.col("price").cast(DoubleType()),
        F.col("freight_value").cast(DoubleType()),
        (F.col("price") + F.col("freight_value")).alias("total_item_value"),
    )

fact_order_items.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("sales_databricks.gold_schema.fact_order_items")
print(f"✓ fact_order_items: {fact_order_items.count()} rows")

# ============================================================
# FACT_PAYMENTS
# ============================================================
fact_payments = order_payments_silver.select(
    F.concat_ws("_", F.col("order_id"),
                F.col("payment_sequential").cast("string")).alias("payment_key"),
    "order_id",
    F.col("payment_sequential").cast(IntegerType()),
    "payment_type",
    F.col("payment_installments").cast(IntegerType()),
    F.col("payment_value").cast(DoubleType()),
)

fact_payments.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("sales_databricks.gold_schema.fact_payments")
print(f"✓ fact_payments: {fact_payments.count()} rows")

# ============================================================
# FACT_ORDER_LIFECYCLE
# ============================================================
payments_agg = order_payments_silver \
    .groupBy("order_id") \
    .agg(
        F.sum("payment_value").alias("total_payment_value"),
        F.countDistinct("payment_type").alias("payment_methods_count")
    )

items_agg = order_items_silver \
    .groupBy("order_id") \
    .agg(
        F.sum("price").alias("total_items_price"),
        F.sum("freight_value").alias("total_freight_value"),
        F.sum(F.col("price") + F.col("freight_value")).alias("total_order_value"),
        F.count("order_item_id").alias("total_items_count"),
    )

fact_order_lifecycle = orders_silver \
    .join(
        order_reviews_silver.select("order_id", "review_id", "review_score"),
        on="order_id", how="left"
    ) \
    .join(payments_agg, on="order_id", how="left") \
    .join(items_agg,    on="order_id", how="left") \
    .select(
        "order_id",
        "customer_id",
        "review_id",
        F.date_format("order_purchase_timestamp", "yyyyMMdd").cast(IntegerType()).alias("date_id"),
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
        F.col("delivery_delay_days").cast(IntegerType()),
        "total_payment_value",
        F.col("payment_methods_count").cast(IntegerType()),
        F.col("review_score").cast(IntegerType()),
        "total_items_price",
        "total_freight_value",
        "total_order_value",
        F.col("total_items_count").cast(IntegerType()),
    ) \
    .withColumn("is_late",
        F.when(F.col("delivery_delay_days") > 0, True).otherwise(False)
    ) \
    .withColumn("is_delivered",
        F.when(F.col("order_status") == "delivered", True).otherwise(False)
    )

fact_order_lifecycle.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("sales_databricks.gold_schema.fact_order_lifecycle")
print(f"✓ fact_order_lifecycle: {fact_order_lifecycle.count()} rows")

# ============================================================
# FINAL SUMMARY
# ============================================================
print("\n" + "="*55)
print("GOLD LAYER COMPLETE")
print("="*55)
for t in ["dim_customers","dim_sellers","dim_products","dim_date",
          "dim_reviews","dim_geolocation",
          "fact_order_items","fact_payments","fact_order_lifecycle"]:
    df = spark.read.table(f"sales_databricks.gold_schema.{t}")
    print(f"✓ {t}: {df.count()} rows | {len(df.columns)} cols")