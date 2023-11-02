import findspark
findspark.init()
from pyspark.sql.functions import from_json, concat_ws, sha2, regexp_replace, col, current_date, expr, year
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from pyspark.sql.functions import explode
from pyspark.sql import DataFrame
from pyspark.conf import SparkConf
SparkSession.builder.config(conf=SparkConf())

mongo_uri = "mongodb://kafka_instalation-mongodb-1:27017" 
mongo_db_name = "db_user"
collection_name = "db_user"
spark = SparkSession.builder \
    .appName("KafkaSparkIntegration") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4," \
            "com.datastax.spark:spark-cassandra-connector_2.12:3.2.0," \
            "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .config("spark.mongodb.output.uri", mongo_uri) \
    .config("spark.mongodb.output.database", mongo_db_name) \
    .config("spark.mongodb.output.collection", collection_name) \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Load data from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "user_data_topic") \
    .option("startingOffsets", "earliest") \
    .load()
value_df = df.selectExpr("CAST(value AS STRING)")

# Convert the value column from Kafka into a string
value_df = df.selectExpr("CAST(value AS STRING)")

data_schema = StructType([
    StructField("results", ArrayType(
        StructType([
            StructField("gender", StringType(), True),
            StructField("name", StructType([
                StructField("title", StringType(), True),
                StructField("first", StringType(), True),
                StructField("last", StringType(), True)
            ]), True),
            StructField("dob", StructType([
                StructField("date", StringType(), True),
                StructField("age", IntegerType(), True)
            ]), True),
           
             StructField("location", StructType([
                StructField("street", StructType([
                    StructField("number", IntegerType(), True),
                    StructField("name", StringType(), True)
                ]), True),
                StructField("city", StringType(), True),
                StructField("state", StringType(), True),
                StructField("country", StringType(), True),
                StructField("postcode", IntegerType(), True)
                ]), True),
            StructField("email", StringType(), True),    
            StructField("login", StructType([
                StructField("uuid", StringType(), True),
                StructField("username", StringType(), True),
            ]), True),
            StructField("registered", StructType([
                StructField("date", StringType(), True)
            ]), True)
        ]), True),
    True)
])
# Parse the JSON data and select relevant fields
selected_df = value_df.withColumn("values", from_json(value_df["value"], data_schema)).selectExpr("explode(values.results) as results_exploded")
result_df = selected_df.select(
    F.col("results_exploded.login.uuid").alias("id"),
    "results_exploded.login.username",
    "results_exploded.gender",
    "results_exploded.name.title",
    "results_exploded.name.first",
    "results_exploded.name.last",
    (year(current_date()) - year(expr("date(results_exploded.dob.date)"))).alias("age"),
    "results_exploded.location.street.number",
    "results_exploded.location.street.name",
    "results_exploded.location.city",
    "results_exploded.location.state",
    "results_exploded.location.country",
    "results_exploded.location.postcode",
    "results_exploded.email",
    F.col("results_exploded.registered.date").alias("inscription"),
)

result_df = result_df.withColumn("full_name", concat_ws(" ", result_df["first"], result_df["last"]))
result_df = result_df.withColumn("full_address", concat_ws(" ", result_df["country"], result_df["state"], result_df["city"], result_df["number"], result_df["postcode"]))

result_df = result_df.drop("first", "last","country", "state","city","name","number","postcode")
result_df = result_df.filter(col("age") > 18)


query = result_df.writeStream \
    .outputMode("append") \
    .foreachBatch(lambda batchDF, batchId: batchDF.write \
        .format("mongo") \
        .option("uri", mongo_uri) \
        .option("database", mongo_db_name) \
        .option("collection", collection_name) \
        .mode("append") \
        .save()
    ) \
    .start()

query.awaitTermination()
