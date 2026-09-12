# Databricks notebook source
# MAGIC %pip install azure-eventhub
# MAGIC import json
# MAGIC from azure.eventhub import EventHubConsumerClient
# MAGIC
# MAGIC conn_str = 'YOUR_EVENTHUBS_CONNECTION_STRING'
# MAGIC eventhub_name = 'test1'
# MAGIC consumer_group = '$Default'
# MAGIC
# MAGIC def on_event(partition_context, event):
# MAGIC     body = event.body_as_str()
# MAGIC     data = json.loads(body)
# MAGIC     
# MAGIC     df = spark.createDataFrame([data])
# MAGIC     
# MAGIC     (df.write 
# MAGIC      .format("delta") 
# MAGIC      .mode("append") 
# MAGIC      .option("mergeSchema", "true") 
# MAGIC      .saveAsTable("sales_databricks.bronze_schema.stream_order_payments"))
# MAGIC     
# MAGIC     print(f"Data processed: {data.get('order_id', 'unknown')}")
# MAGIC
# MAGIC consumer = EventHubConsumerClient.from_connection_string(
# MAGIC     conn_str=conn_str,
# MAGIC     consumer_group=consumer_group, 
# MAGIC     eventhub_name=eventhub_name
# MAGIC )
# MAGIC
# MAGIC with consumer:
# MAGIC     consumer.receive(
# MAGIC         on_event=on_event,
# MAGIC         starting_position="-1"
# MAGIC     )