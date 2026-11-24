import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


def main():
    t_start = time.time()

    spark = (
        SparkSession.builder.appName("Part5-Production-Spark-Kafka-Pipeline")
        .master("spark://spark-master:7077")
        .config("spark.sql.shuffle.partitions", "8")
        # Support Delta Lake (à passer aussi via --packages io.delta:delta-spark_2.12:3.1.0)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    orders_schema = StructType([
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

    restaurants_schema = StructType([
        StructField("restaurant_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("city", StringType(), True),
        StructField("category", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
    ])

    # Référentiel statique restaurants pour jointure stream-static
    df_restos_static = (
        spark.read.schema(restaurants_schema)
        .option("multiline", True)
        .json("/workspace/data/restaurants.json")
        .filter(F.col("restaurant_id").isNotNull())
        .dropDuplicates(["restaurant_id"])
        .select(
            "restaurant_id",
            F.col("name").alias("restaurant_name"),
            "category",
            "latitude",
            "longitude",
        )
    )

    # 1. Lecture du flux Kafka (topic orders)
    df_kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "orders")
        .option("startingOffsets", "earliest")
        .load()
    )

    df_parsed = (
        df_kafka.selectExpr("CAST(value AS STRING) AS raw_json")
        .select(F.from_json(F.col("raw_json"), orders_schema).alias("d"))
        .select("d.*")
        .withColumn("event_time", F.to_timestamp("event_time"))
        .filter(
            F.col("order_id").isNotNull()
            & F.col("restaurant_id").isNotNull()
            & F.col("city").isNotNull()
            & F.col("event_time").isNotNull()
        )
        # 2. Watermark (10 min) + déduplication sur (order_id, status)
        .withWatermark("event_time", "10 minutes")
        .dropDuplicates(["order_id", "status"])
    )

    # 3. Jointure stream-static + distance_band + date
    df_enriched = (
        df_parsed.join(df_restos_static, on="restaurant_id", how="left")
        .withColumn(
            "distance_band",
            F.when(F.col("distance_km") < 2.0, "court")
            .when(F.col("distance_km") <= 5.0, "moyen")
            .otherwise("long"),
        )
        .withColumn("date", F.to_date("event_time"))
        .withColumn("processed_at", F.current_timestamp())
    )

    # 4. Sink Silver Parquet partitionné par city et date
    query_silver = (
        df_enriched.writeStream.format("parquet")
        .outputMode("append")
        .option("path", "/workspace/lake/silver/orders_clean")
        .option("checkpointLocation", "/workspace/lake/checkpoints/part5_silver_clean")
        .partitionBy("city", "date")
        .start()
    )

    # 5. Fenêtre tumbling (10 min) par ville -> Sink Gold Delta Lake
    df_tumbling_10m = (
        df_enriched.groupBy(
            F.window(F.col("event_time"), "10 minutes"),
            F.col("city"),
        )
        .agg(
            F.count("order_id").alias("nb_commandes"),
            F.round(F.sum("amount_mad"), 2).alias("ca_mad"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "city",
            "nb_commandes",
            "ca_mad",
        )
    )

    query_gold_delta = (
        df_tumbling_10m.writeStream.format("delta")
        .outputMode("append")
        .option("path", "/workspace/lake/gold/kpi_city_10min")
        .option("checkpointLocation", "/workspace/lake/checkpoints/part5_gold_kpi_10min")
        .start()
    )

    # 6. Fenêtre sliding (10 min, glissement 5 min) -> taux d'annulation par ville
    df_sliding_cancel = (
        df_enriched.groupBy(
            F.window(F.col("event_time"), "10 minutes", "5 minutes"),
            F.col("city"),
        )
        .agg(
            F.count("*").alias("total_events"),
            F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0)).alias("nb_cancelled"),
        )
        .withColumn(
            "taux_annulation_pct",
            F.when(
                F.col("total_events") > 0,
                F.round((F.col("nb_cancelled") / F.col("total_events")) * 100, 2),
            ).otherwise(0.0),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "city",
            "total_events",
            "nb_cancelled",
            "taux_annulation_pct",
        )
    )

    query_sliding = (
        df_sliding_cancel.writeStream.format("console")
        .outputMode("update")
        .option("truncate", "false")
        .start()
    )

    # 7. Fenêtre session (inactivité 5 min) par courier_id
    df_session_courier = (
        df_enriched.groupBy(
            F.session_window(F.col("event_time"), "5 minutes"),
            F.col("courier_id"),
        )
        .agg(
            F.count("order_id").alias("nb_evenements"),
        )
        .withColumn(
            "duree_session_sec",
            F.unix_timestamp(F.col("session_window.end"))
            - F.unix_timestamp(F.col("session_window.start")),
        )
        .select(
            F.col("session_window.start").alias("session_start"),
            F.col("session_window.end").alias("session_end"),
            "courier_id",
            "nb_evenements",
            "duree_session_sec",
        )
    )

    query_session = (
        df_session_courier.writeStream.format("console")
        .outputMode("update")
        .option("truncate", "false")
        .start()
    )

    print(f"Requêtes Partie 5 démarrées en {time.time() - t_start:.2f} s.")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()