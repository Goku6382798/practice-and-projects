from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from delta.tables import DeltaTable
import logging
import boto3
import sys
from awsglue.utils import getResolvedOptions

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = getResolvedOptions(sys.argv, ["table_name"])
table_name = args["table_name"]

spark = SparkSession.builder \
    .appName("Delta Lake Upsert Data Aggregations") \
    .getOrCreate()

s3_bucket_path = "s3://gd-aws-de-labs/"
input_path = f"{s3_bucket_path}/raw_landing_zone/flights_db/{table_name}/"
output_path = f"{s3_bucket_path}/lakehouse-dwh/{table_name}"

primary_key = "flight_id"

df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(input_path)

if table_name=='flights':
    df = df.withColumn("flight_date", to_date(col("created_at")))

logger.info(f"DataFrame schema for table {table_name}: {df.dtypes}")

delta_table_exists = DeltaTable.isDeltaTable(spark, output_path)

if delta_table_exists:
    delta_table = DeltaTable.forPath(spark, output_path)
    
    logger.info(f"Delta table schema: {delta_table.toDF().dtypes}")
    
    merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in primary_key])
    delta_table.alias("target").merge(
        df.alias("source"),
        merge_condition
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    if table_name=='flights':
        df.write.format("delta").partitionBy("flight_date").mode("overwrite").save(output_path)
    else:
        df.write.format("delta").mode("overwrite").save(output_path)

s3 = boto3.client('s3')
bucket_name = s3_bucket_path.split('/')[2]
input_prefix = f"raw_landing_zone/{table_name}/"
archive_prefix = f"archived/{table_name}/"

try:
    objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=input_prefix).get('Contents', [])
    for obj in objects:
        source_key = obj['Key']
        destination_key = source_key.replace(input_prefix, archive_prefix)
        copy_source = {'Bucket': bucket_name, 'Key': source_key}
        s3.copy_object(CopySource=copy_source, Bucket=bucket_name, Key=destination_key)
        s3.delete_object(Bucket=bucket_name, Key=source_key)
    logger.info("Files moved to archive successfully.")
except Exception as e:
    logger.error(f"Error moving files to archive: {e}")


spark.stop()
