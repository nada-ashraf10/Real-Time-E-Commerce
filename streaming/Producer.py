# Databricks notebook source
# MAGIC %pip install azure-eventhub
# MAGIC import azure.eventhub
# MAGIC import json
# MAGIC import pandas as pd
# MAGIC import time
# MAGIC from azure.eventhub import EventHubProducerClient, EventData
# MAGIC conn_str = 'YOUR_EVENTHUBS_CONNECTION_STRING'
# MAGIC
# MAGIC producer = EventHubProducerClient.from_connection_string(
# MAGIC     conn_str=conn_str,
# MAGIC     eventhub_name=eventhub_name
# MAGIC )
# MAGIC
# MAGIC df = pd.read_csv("/Volumes/sales_databricks/bronze_schema/raw_files/olist_order_payments_dataset.csv")
# MAGIC
# MAGIC with producer:
# MAGIC     for index, row in df.iterrows():
# MAGIC         payload = row.to_dict()
# MAGIC         data = EventData(json.dumps(payload))
# MAGIC         producer.send_event(data)
# MAGIC         
# MAGIC         print(f"Payment sent for order{payload.get('order_id')}")
# MAGIC         time.sleep(1)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sales_databricks.bronze_schema.stream_order_payments;