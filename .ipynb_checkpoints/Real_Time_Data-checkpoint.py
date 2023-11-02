import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import  from_json, concat_ws, sha2, regexp_replace,col, current_date, expr, year
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from pyspark.conf import SparkConf
SparkSession.builder.config(conf=SparkConf())
from pyspark.sql.functions import explode,from_json
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from uuid import uuid4
import pyspark.sql.functions as F
#cassndra
keyspaceName = 'user_keyspace'
tableName = 'data_users_table'
#mongodb
mongo_uri = "mongodb://kafka_instalation-mongodb-1:27017" 
mongo_db_name = "db_user"
collection_name = "db_user"


# Initialize a Spark session
spark = SparkSession.builder \
    .appName("KafkaCassandraIntegration") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4,com.datastax.spark:spark-cassandra-connector_2.12:3.2.0,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .config('spark.cassandra.connection.host', 'kafka_instalation-cassandra-1') \
    .config("spark.mongodb.output.uri", mongo_uri) \
    .config("spark.mongodb.output.database", mongo_db_name) \
    .config("spark.mongodb.output.collection", collection_name) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# Define the Kafka source
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "user_data_topic") \
    .option("startingOffsets", "earliest") \
    .load()

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

#transformation
result_df = result_df.withColumn("full_name", concat_ws(" ", result_df["first"], result_df["last"]))
result_df = result_df.withColumn("full_address", concat_ws(" ", result_df["country"], result_df["state"], result_df["city"], result_df["number"], result_df["postcode"]))
result_df = result_df.drop("first", "last","country", "state","city","name","number","postcode")

#RGPD
result_df = result_df.filter(col("age") > 18)
#________________________________________________________________cassandra__________________________________________________________________

#connect_to_cassandra
auth_provider = PlainTextAuthProvider(username='', password='')
cluster = Cluster(['kafka_instalation-cassandra-1'], auth_provider=auth_provider)
session = cluster.connect()

#create_cassandra_keyspace
create_keyspace_query = """ CREATE KEYSPACE IF NOT EXISTS """+keyspaceName+ \
""" WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}"""
session.execute(create_keyspace_query)

#create_cassandra_table
create_table_query = f"""
CREATE TABLE IF NOT EXISTS {keyspaceName}.{tableName} (
    id UUID PRIMARY KEY,
    username TEXT,
    gender TEXT,
    title TEXT,
    age INT,
    email TEXT,
    inscription TEXT,
    full_name TEXT,
    full_address TEXT
)
"""
session.execute(create_table_query)
result_df = result_df.filter("id IS NOT NULL")

result_df.writeStream \
    .outputMode("append") \
    .format("org.apache.spark.sql.cassandra") \
    .option("checkpointLocation", "./checkpoint/data") \
    .option("keyspace", keyspaceName) \
    .option("table", tableName) \
    .start()
#________________________________________________________________mongodb__________________________________________________________________
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

query = result_df.writeStream.outputMode("append").format("console").start()
query.awaitTermination()
