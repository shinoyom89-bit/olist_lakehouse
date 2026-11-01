from pyspark import pipelines as dp
from pyspark.sql.functions import *

# dbutils.widgets.text("catalog", "")

@dp.table(name="silver_orders")
@dp.expect("valid_order_id", "order_id IS NOT NULL")
@dp.expect_all_or_drop({
    "valid_customer_id": "customer_id IS NOT NULL",
    "valid_order_status": "order_status IS NOT NULL",
    "valid_order_approved_at": "order_approved_at IS NOT NULL",
    "valid_updated_at": "updated_at IS NOT NULL",
    "valid_estimated_time": "order_estimated_delivery_date IS NOT NULL",
    "valid_order_purchase_timestamp": "order_purchase_timestamp IS NOT NULL",
    "valid_order_delivered_customer_date": "order_delivered_customer_date IS NOT NULL"
})
def silver_order():

    df = spark.readStream.table(
        "olist_dev.bronze.orders_bronze"
    )

    df = (
        df.withColumn("transform_at", current_timestamp())
          .withColumn("order_status", initcap(col("order_status")))\
              .withColumn("")
    )

    return df