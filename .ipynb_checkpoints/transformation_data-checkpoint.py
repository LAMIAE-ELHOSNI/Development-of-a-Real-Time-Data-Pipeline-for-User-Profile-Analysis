import findspark
findspark.init()
from pyspark.sql.functions import from_json, concat_ws, sha2, regexp_replace,col, current_date, expr, year
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from pyspark.sql.functions import explode


# Initialize a Spark session
spark = SparkSession.builder \
    .appName("KafkaSparkIntegration") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4") \
    .getOrCreate()

# Load data from Kafka
df = spark.readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka:9092") \
  .option("subscribe", "user_data_topic") \
  .option("startingOffsets", "earliest") \
  .load()

value_df = df.selectExpr("CAST(value AS STRING)")

# Define a simplified schema
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
            StructField("registered", StructType([
                StructField("date", StringType(), True)
            ]), True)
        ]), True),
    True)
])
selected_df = value_df.withColumn("values", from_json(value_df["value"], data_schema)).selectExpr("explode(values.results) as results_exploded")
result_df = selected_df.select(
    "results_exploded.gender",
    "results_exploded.name.title",
    "results_exploded.name.first",
    "results_exploded.name.last",
    (year(current_date()) - year(expr("date(results_exploded.dob.date)"))).alias("age"),
    #F.expr("year(current_date()) - year(to_date(regexp_replace(results_exploded.dob.date, 'T', ' '), 'yyyy-MM-dd HH:mm:ss.SSS''Z'))").alias("age"),
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

result_df = result_df.withColumn("full_adress", concat_ws(" ", result_df["country"], result_df["state"], result_df["city"],result_df['name'],result_df['number'],result_df["postcode"]))


# Add a condition to filter rows with age greater than 18

# Remove unwanted columns
result_df = result_df.drop("first", "last")
result_df = result_df.drop("country", "state","city","name","number","postcode")
result_df =result_df.filter(col("age") > 60)
# Remove null values
result_df = result_df.na.drop()

query = result_df.writeStream.outputMode("append").format("console").start()

query.awaitTermination()
