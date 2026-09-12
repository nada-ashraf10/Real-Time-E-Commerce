# Databricks notebook source
spark.sql("USE CATALOG sales_databricks")

# COMMAND ----------

#customers
from pyspark.sql.functions import col, trim, upper, initcap, row_number
from pyspark.sql.window import Window

customers = spark.table("bronze_schema.customers")

window = Window.partitionBy("customer_id").orderBy("customer_id")

customers_silver = customers.filter(col("customer_id").isNotNull()).withColumn("customer_city", initcap(trim(col("customer_city")))).withColumn("customer_state", upper(trim(col("customer_state")))).withColumn("rn", row_number().over(window)).filter(col("rn") == 1).drop("rn")

customers_silver.write.format("delta").mode("overwrite").saveAsTable("silver_schema.customers")

print(f"customers done: {customers_silver.count()} rows")

# COMMAND ----------

#geolocation
from pyspark.sql.functions import col, trim, upper, initcap

geolocation = spark.table("bronze_schema.geolocation")

geolocation_silver = geolocation.filter(col("geolocation_zip_code_prefix").isNotNull()).withColumn("geolocation_city", initcap(trim(col("geolocation_city")))).withColumn("geolocation_state", upper(trim(col("geolocation_state")))).withColumn("geolocation_lat", col("geolocation_lat").cast("double")).withColumn("geolocation_lng", col("geolocation_lng").cast("double")).dropDuplicates(["geolocation_zip_code_prefix"])

geolocation_silver.write.format("delta").mode("overwrite").saveAsTable("silver_schema.geolocation")

print(f"geolocation done: {geolocation_silver.count()} rows")

# COMMAND ----------

# order_items
from pyspark.sql.functions import col, to_timestamp, row_number
from pyspark.sql.window import Window

order_items = spark.table("bronze_schema.order_items")

window = Window.partitionBy("order_id", "order_item_id").orderBy("order_id")

order_items_silver = order_items.filter(col("order_id").isNotNull()).filter(col("price").cast("double") > 0).withColumn("shipping_limit_date", to_timestamp(col("shipping_limit_date"))).withColumn("price", col("price").cast("double")).withColumn("freight_value", col("freight_value").cast("double")).withColumn("rn", row_number().over(window)).filter(col("rn") == 1).drop("rn")

order_items_silver.write.format("delta").mode("overwrite").saveAsTable("silver_schema.order_items")

print(f"order_items done: {order_items_silver.count()} rows")

# COMMAND ----------

#n
#product_category_translation table
from pyspark.sql.functions import col, trim, lower, length

bronze_translation_df = spark.table("sales_databricks.bronze_schema.product_category_translation")

silver_translation_df = (
    bronze_translation_df
 
    .withColumn("product_category_name", lower(trim(col("product_category_name"))))
    .withColumn("product_category_name_english", lower(trim(col("product_category_name_english"))))
    .withColumn("product_category_name", col("product_category_name").cast("string"))
    .withColumn("product_category_name_english", col("product_category_name_english").cast("string"))

    .dropna(subset=["product_category_name", "product_category_name_english"])

    .dropDuplicates(subset=["product_category_name"])

    .filter(
        (length(col("product_category_name")) > 0) & 
        (length(col("product_category_name_english")) > 0)
    )
)


silver_translation_df.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("sales_databricks.silver_schema.product_category_translation")

print("Successfully cleaned and saved 'product_category_translation' to the Silver schema.")
display(silver_translation_df)

# COMMAND ----------

#products table
from pyspark.sql.functions import col, when, trim
bronze_products_df = spark.table("sales_databricks.bronze_schema.products")
silver_products_df = (
    bronze_products_df
    .withColumnRenamed("product_name_lenght", "product_name_length")
    .withColumnRenamed("product_description_lenght", "product_description_length")
    .withColumn("product_category_name", 
        when(trim(col("product_category_name")) == "", "unknown")
        .when(col("product_category_name").isNull(), "unknown")
        .otherwise(col("product_category_name"))
    )
    .withColumn("product_name_length", col("product_name_length").cast("integer"))
    .withColumn("product_description_length", col("product_description_length").cast("integer"))
    .withColumn("product_photos_qty", col("product_photos_qty").cast("integer"))
    .withColumn("product_weight_g", col("product_weight_g").cast("double"))
    .withColumn("product_length_cm", col("product_length_cm").cast("double"))
    .withColumn("product_height_cm", col("product_height_cm").cast("double"))
    .withColumn("product_width_cm", col("product_width_cm").cast("double"))
    .fillna({
        "product_name_length": 0,
        "product_description_length": 0,
        "product_photos_qty": 0,
        "product_weight_g": 0.0,
        "product_length_cm": 0.0,
        "product_height_cm": 0.0,
        "product_width_cm": 0.0
    })
    .dropna(subset=["product_id"])
    .dropDuplicates(subset=["product_id"])
    .filter(col("product_weight_g") >= 0)
)
silver_products_df.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("sales_databricks.silver_schema.products")
print("Successfully cleaned and saved 'products' to the Silver schema.")
display(silver_products_df)

# COMMAND ----------

#seller table
from pyspark.sql.functions import col, trim, lower, upper, length, lpad
bronze_sellers_df = spark.table("sales_databricks.bronze_schema.sellers")
silver_sellers_df = (
    bronze_sellers_df
    .withColumn("seller_city", lower(trim(col("seller_city"))))
    .withColumn("seller_state", upper(trim(col("seller_state"))))
    .withColumn("seller_id", col("seller_id").cast("string"))
    .withColumn("seller_zip_code_prefix", lpad(col("seller_zip_code_prefix").cast("string"), 5, "0"))
    .withColumn("seller_city", col("seller_city").cast("string"))
    .withColumn("seller_state", col("seller_state").cast("string"))
    .dropna(subset=["seller_id"])
    .dropDuplicates(subset=["seller_id"])
    .filter(length(col("seller_state")) == 2)
)

silver_sellers_df.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("sales_databricks.silver_schema.sellers")
print("Successfully cleaned and saved 'sellers' to the Silver schema.")
display(silver_sellers_df)

# COMMAND ----------

#orders
from pyspark.sql import functions as F

orders = spark.table("sales_databricks.bronze_schema.orders")

orders_silver = (
    orders
    .dropDuplicates(["order_id"])
    .filter(F.col("order_id").isNotNull())
    .filter(F.col("customer_id").isNotNull())
    .filter(F.col("order_status").isNotNull())
    .withColumn("order_purchase_timestamp",       F.to_timestamp("order_purchase_timestamp"))
    .withColumn("order_approved_at",              F.to_timestamp("order_approved_at"))
    .withColumn("order_delivered_carrier_date",   F.to_timestamp("order_delivered_carrier_date"))
    .withColumn("order_delivered_customer_date",  F.to_timestamp("order_delivered_customer_date"))
    .withColumn("order_estimated_delivery_date",  F.to_timestamp("order_estimated_delivery_date"))
    .withColumn("delivery_delay_days",
        F.datediff(
            F.col("order_delivered_customer_date"),
            F.col("order_estimated_delivery_date")
        )
    )
)

orders_silver.write.format("delta").mode("overwrite").saveAsTable("sales_databricks.silver_schema.orders")
print(f"✓ orders: {orders_silver.count()} rows")
display(orders_silver)

# COMMAND ----------

#order_payments
from pyspark.sql.types import DoubleType, IntegerType

order_payments = spark.table("sales_databricks.bronze_schema.order_payments")

order_payments_silver = (
    order_payments
    .dropDuplicates(["order_id", "payment_sequential"])
    .filter(F.col("order_id").isNotNull())
    .withColumn("payment_value",        F.col("payment_value").cast(DoubleType()))
    .withColumn("payment_installments", F.col("payment_installments").cast(IntegerType()))
    .filter(F.col("payment_value") > 0)
    .withColumn("payment_type", F.lower(F.trim(F.col("payment_type"))))
)

order_payments_silver.write.format("delta").mode("overwrite").saveAsTable("sales_databricks.silver_schema.order_payments")
print(f"✓ order_payments: {order_payments_silver.count()} rows")
display(order_payments_silver)

# COMMAND ----------


#order_reviews

order_reviews = spark.table("sales_databricks.bronze_schema.order_reviews")

order_reviews_silver = (
    order_reviews
    .dropDuplicates(["review_id"])
    .filter(F.col("order_id").isNotNull())
    .withColumn("review_score", F.expr("try_cast(review_score as INT)"))
    .filter(F.col("review_score").between(1, 5))
    .withColumn("review_creation_date", F.expr("try_cast(review_creation_date as TIMESTAMP)"))
    .withColumn("review_answer_timestamp", F.expr("try_cast(review_answer_timestamp as TIMESTAMP)"))
    .withColumn("review_comment_message",
        F.when(F.col("review_comment_message").isNull(), "no comment")
         .otherwise(F.col("review_comment_message"))
    )
)

order_reviews_silver.write.format("delta").mode("overwrite").saveAsTable("sales_databricks.silver_schema.order_reviews")
print(f"✓ order_reviews: {order_reviews_silver.count()} rows")
display(order_reviews_silver)

# COMMAND ----------

#TESTING
for t in ["orders", "order_payments", "order_reviews"]:
    df = spark.table(f"sales_databricks.silver_schema.{t}")
    print(f"{t}: {df.count()} rows | {len(df.columns)} cols")

# COMMAND ----------

#final test->critical columns
spark.table("sales_databricks.silver_schema.orders") \
    .filter(F.col("order_id").isNull() | F.col("customer_id").isNull()).count()

spark.table("sales_databricks.silver_schema.order_payments") \
    .filter(F.col("order_id").isNull() | F.col("payment_value").isNull()).count()

spark.table("sales_databricks.silver_schema.order_reviews") \
    .filter(F.col("order_id").isNull() | F.col("review_id").isNull()).count()