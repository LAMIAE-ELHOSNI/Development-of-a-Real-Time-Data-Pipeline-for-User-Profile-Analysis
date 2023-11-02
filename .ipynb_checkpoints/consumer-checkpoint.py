import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("KafkaSparkIntegration") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4") \
    .getOrCreate()

# Define the schema for JSON data
data_schema = StructType([
    StructField("results", ArrayType(
        StructType([
            StructField("gender", StringType(), True),
            StructField("name", StructType([
                StructField("title", StringType(), True),
                StructField("first", StringType(), True),
                StructField("last", StringType(), True)
            ]), True),
            StructField("location", StructType([
                StructField("country", StringType(), True),
                StructField("city", StringType(), True),
            ]), True),
            StructField("registered", StructType([
                StructField("date", StringType(), True),
            ]), True),
        ]), True),
    True)
])

# Read data from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "user_data_topic") \
    .load()

# Deserialize the message value from Kafka (assuming it's in JSON format)
df = df.selectExpr("CAST(value AS STRING)")

# Parse the JSON data with error handling
try:
    df = df.withColumn("values", from_json(df["value"], data_schema)).select('values.results')
except Exception as e:
    print(f"Error parsing JSON data: {str(e)}")

# Display the data to the console
query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
