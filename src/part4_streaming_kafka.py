from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

spark = (
    SparkSession.builder.appName("Part4-Structured-Streaming-Kafka")
    .master("spark://spark-master:7077")
    .config("spark.sql.shuffle.partitions", "3")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

order_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("restaurant_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("city", StringType(), True),
    StructField("status", StringType(), True),
    StructField("amount_mad", DoubleType(), True),
    StructField("items_count", IntegerType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("courier_id", StringType(), True),
    StructField("event_time", StringType(), True),
])

# 1. Lecture du flux Kafka
raw_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "orders")
    .option("startingOffsets", "latest")
    .load()
)

# 2. Parsing JSON + conversion event_time en TimestampType
parsed_stream = (
    raw_stream.selectExpr("CAST(value AS STRING) AS json_str", "timestamp AS kafka_ts")
    .select(F.from_json(F.col("json_str"), order_schema).alias("data"), "kafka_ts")
    .select("data.*", "kafka_ts")
    .withColumn("event_ts", F.to_timestamp(F.col("event_time")))
    .filter(F.col("event_ts").isNotNull())
)

# 3. Agrégation par fenêtre glissante (5 minutes, glissement 1 minute) + watermark (10 minutes)
windowed_agg = (
    parsed_stream.filter(F.col("status") == "DELIVERED")
    .withWatermark("event_ts", "10 minutes")
    .groupBy(
        F.window(F.col("event_ts"), "5 minutes", "1 minute").alias("window"),
        F.col("city"),
    )
    .agg(
        F.round(F.sum("amount_mad"), 2).alias("ca_fenetre"),
        F.count("*").alias("nb_livraisons"),
    )
    .select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        F.col("city"),
        F.col("ca_fenetre"),
        F.col("nb_livraisons"),
    )
)

# 4. Écriture continue en Parquet avec checkpoint
query = (
    windowed_agg.writeStream.format("parquet")
    .option("path", "/workspace/lake/streaming/orders_windowed")
    .option("checkpointLocation", "/workspace/lake/checkpoints/orders_streaming")
    .outputMode("append")
    .start()
)

print("Streaming démarré... Envoyez des messages depuis producer.py --mode kafka.")
query.awaitTermination()