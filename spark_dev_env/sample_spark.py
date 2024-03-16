# Sample Spark application to test spark behaviours on a development environment so
# I can use in Streame
from pyspark.sql import SparkSession
from pyspark.sql.functions import window, sum
from pyspark.sql.types import StructType, StructField, TimestampType, IntegerType

# Initialize Spark Session
spark = (
    SparkSession.builder.appName("Sum Inputs in 2 Minute Windows with Watermark")
    .master("local[*]")
    .getOrCreate()
)

# Define the schema of the input data
schema = StructType(
    [
        StructField("timestamp", TimestampType(), True),
        StructField("value", IntegerType(), True),
    ]
)

# Create DataFrame representing the stream of input lines from connection to localhost:9999
df = (
    spark.readStream.format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()
    .selectExpr("CAST(value AS STRING)")
)

# Assuming the stream contains timestamp and value in CSV format, let's parse it
imported_df = df.selectExpr(
    "split(value, ',')[0] as timestamp", "split(value, ',')[1] as value"
).selectExpr("CAST(timestamp AS TIMESTAMP)", "CAST(value AS INT)")

# Group data by 2 minute window and sum the values, with watermark
summed_df = (
    imported_df.withWatermark("timestamp", "10 minutes")
    .groupBy(window(imported_df.timestamp, "2 minutes"))
    .agg(sum("value").alias("sum_values"))
)

# Start running the query that prints the running counts to the console
query = summed_df.writeStream.format("console").outputMode("update").start()

query.awaitTermination()
