from pyspark import pipelines as dp
from pyspark.sql.functions import *
# dbutils.widgets.text("catalog", "")
@dp.temporary_view(name="sellers_cleaned")
@dp.expect_all_or_drop({
    "seller_id_not_null": "seller_id IS NOT NULL",
    "updated_at_not_null": "updated_at IS NOT NULL",
    "seller_zip_not_null": "seller_zip_code_prefix IS NOT NULL"
})
def sellers_cleaned():
    df = spark.readStream.table("olist_dev.bronze.sellers_bronze")
    return (
        df.withColumn("transformed_At", current_timestamp())
          .withColumn("seller_city", initcap(col("seller_city")))\
              .withColumn("seller_state", initcap(col("seller_state")))
    )
dp.create_streaming_table("silver_sellers")
dp.create_auto_cdc_flow(
    target="silver_sellers",
    source="sellers_cleaned",
    keys=["seller_id"],
    sequence_by=col("updated_at"),
    stored_as_scd_type=1
)