from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    FloatType,
)

# SCHEMAS

stediAppSchema = StructType(
    [
        StructField("customer", StringType()),
        StructField("score", FloatType()),
        StructField("riskDate", StringType()),
    ]
)

# SPARK INITIAL CONFIG

spark = SparkSession.builder.appName("customer-record").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# READING TOPIC DATA SOURCE - INITIAL STREAMING DF
stediAppRawStreamingDF = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:19092")
    .option("subscribe", "stedi-events")
    .option("startingOffsets", "earliest")
    .load()
)

# DECODING JSON AND CREATING A VIEW

stediAppStreamingDF = stediAppRawStreamingDF.selectExpr(
    "cast(key as string) key", "cast(value as string) value"
)

stediAppStreamingDF.withColumn("value", from_json("value", stediAppSchema)).select(
    col("value.*")
).createOrReplaceTempView("CustomerRisk")

# SELECTING THE COLUMNS AND WRITING IT INTO THE STREAM

customerRiskStreamingDF = spark.sql("select customer, score from CustomerRisk")

customerRiskStreamingDF.writeStream.outputMode("append").format(
    "console"
).start().awaitTermination()
