from pyspark import pipelines as dp
from pyspark.sql.functions import *
# dbutils.widgets.text("catalog", "")
@dp.temporary_view(name="customer_cleaned")
@dp.expect_all_or_drop({
    "customer_id_valid": "customer_id IS NOT NULL",
    "valid_update": " ",
    "": ""
})
def sellers_cleaned():
    df = spark.readStream.table
dp.create_streaming_table("silver_customers")
dp.create_auto_cdc_flow(
    target="silver_customers",
    source="sellers_cleaned",
    keys=["customer_id"],
    sequence_by=col("updated_at"),
    stored_as_scd_type=1
)