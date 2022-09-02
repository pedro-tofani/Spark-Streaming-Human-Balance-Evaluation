from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    ArrayType,
    FloatType,
)


# SCHEMAS

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

spark = SparkSession.builder.appName("customer-redis").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

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

# STREAMING IT INTO THE CONSOLE

emailAndBirthYearStreamingDF.writeStream.outputMode("append").format(
    "console"
).start().awaitTermination()