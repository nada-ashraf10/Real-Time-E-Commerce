# Databricks notebook source
base_path = "/Volumes/sales_databricks/bronze_schema/raw_files/"
bronze_files = {
    "customers": "olist_customers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "product_category_translation": "product_category_name_translation.csv"
}


for table_name, file_name in bronze_files.items():
    print(f"Processing: {file_name} -> bronze.{table_name}")
    
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(base_path + file_name)
    
    df.write.mode("overwrite") \
        .format("delta") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"sales_databricks.bronze_schema.{table_name}")