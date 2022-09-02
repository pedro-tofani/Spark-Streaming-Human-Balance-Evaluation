from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    BooleanType,
    ArrayType,
    DateType,
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

redisSchema = StructType(
    [
        StructField("key", StringType()),
        StructField(
            "zSetEntries",
            ArrayType(
                StructType(
                    [
                        StructField("element", StringType()),
                        StructField("score", FloatType()),
                    ]
                )
            ),
        ),
    ]
)

customerRecordsSchema = StructType(
    [
        StructField("customerName", StringType()),
        StructField("email", StringType()),
        StructField("phone", StringType()),
        StructField("birthDay", StringType()),
    ]
)


# SPARK INITIAL CONFIG

spark = SparkSession.builder.appName("stedi-app").getOrCreate()
spark.sparkContext.setLogLevel("WARN")


# Topic 1: redis-server

# READING TOPIC DATA SOURCE - INITIAL STREAMING DF

redisRawStreamingDF = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:19092")
    .option("subscribe", "redis-server")
    .option("startingOffsets", "earliest")
    .load()
)

# DECODING JSON AND CREATING A VIEW

redisStreamingDF = redisRawStreamingDF.selectExpr("cast(value as string) value")

redisStreamingDF.withColumn("value", from_json("value", redisSchema)).select(
    col("value.*")
).createOrReplaceTempView("RedisSortedSet")

# DECODING CUSTOMER COLUMN

zSetEntriesEncodedStreamingDF = spark.sql(
    "select key, zSetEntries[0].element as encodedCustomer from RedisSortedSet"
)

zSetDecodedEntriesStreamingDF = zSetEntriesEncodedStreamingDF.withColumn(
    "customer", unbase64(zSetEntriesEncodedStreamingDF.encodedCustomer).cast("string")
)

zSetDecodedEntriesStreamingDF.withColumn(
    "customer", from_json("customer", customerRecordsSchema)
).select(col("customer.*")).createOrReplaceTempView("CustomerRecords")


# SELECTING ONLY THE EMAIL AND BIRTHDAY FIELDS THAT AREN'T null

emailAndBirthDayStreamingDF = spark.sql(
    "select * from CustomerRecords where email is not null AND birthDay is not null"
)

## CONVERTING THE FIELD TO GET THE BIRTH YEAR

emailAndBirthYearStreamingDF = emailAndBirthDayStreamingDF.select(
    "email",
    split(emailAndBirthDayStreamingDF.birthDay, "-").getItem(0).alias("birthYear"),
)

# Topic 2: stedi-events

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
    "cast(value as string) value"
)

stediAppStreamingDF.withColumn("value", from_json("value", stediAppSchema)).select(
    col("value.*")
).createOrReplaceTempView("CustomerRisk")

# SELECTING THE COLUMNS AND WRITING IT INTO THE STREAM

customerRiskStreamingDF = spark.sql("select customer, score from CustomerRisk")


# Topic 3: stedi-graph

# JOINING THE TWO STREAMINGS DF


joinedCustomerRiskAndBirthDf = customerRiskStreamingDF.join(emailAndBirthYearStreamingDF, expr( """
   customer = email
"""
))

# STREAMING THE JOINNED DATAFRAME

(
  joinedCustomerRiskAndBirthDf
    .selectExpr("CAST(customer AS STRING) AS key", "to_json(struct(*)) AS value")
    .writeStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:19092")
    .option("topic", "stedi-graph")
    .option("checkpointLocation","/tmp/checkPointKafka")
    .start()
    .awaitTermination()
)