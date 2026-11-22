import os
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
        SparkSession.builder.appName("Part5-Production-Lakehouse-Pipeline")
        .master("spark://spark-master:7077")
        .config("spark.sql.shuffle.partitions", "8")
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

    # =========================================================================
    # 1. COUCHE BRONZE : Ingestion brute horodatée
    # =========================================================================
    print("--- [BRONZE] Ingestion des sources brutes ---")
    df_orders_raw = (
        spark.read.schema(orders_schema)
        .json("/workspace/data/orders_history.jsonl")
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("source_file", F.lit("orders_history.jsonl"))
    )

    df_restos_raw = (
        spark.read.schema(restaurants_schema)
        .option("multiline", True)
        .json("/workspace/data/restaurants.json")
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("source_file", F.lit("restaurants.json"))
    )

    df_orders_raw.write.mode("overwrite").parquet("/workspace/lake/bronze/orders")
    df_restos_raw.write.mode("overwrite").parquet(
        "/workspace/lake/bronze/restaurants"
    )
    print(f"Bronze orders count : {df_orders_raw.count()}")
    print(f"Bronze restaurants count : {df_restos_raw.count()}")

    # =========================================================================
    # 2. COUCHE SILVER : Nettoyage, déduplication, typage et enrichissement
    # =========================================================================
    print("--- [SILVER] Nettoyage et enrichissement ---")
    df_bronze_orders = spark.read.parquet("/workspace/lake/bronze/orders")
    df_bronze_restos = spark.read.parquet("/workspace/lake/bronze/restaurants")

    df_orders_clean = (
        df_bronze_orders.filter(
            F.col("order_id").isNotNull()
            & F.col("restaurant_id").isNotNull()
            & F.col("city").isNotNull()
            & (F.col("amount_mad") > 0)
            & (F.col("distance_km") >= 0)
        )
        .withColumn(
            "event_ts",
            F.coalesce(
                F.to_timestamp("event_time"),
                F.current_timestamp(),
            ),
        )
        .withColumn("order_date", F.to_date("event_ts"))
        .dropDuplicates(["order_id"])
    )

    df_restos_clean = (
        df_bronze_restos.filter(F.col("restaurant_id").isNotNull())
        .dropDuplicates(["restaurant_id"])
        .select(
            "restaurant_id",
            F.col("name").alias("restaurant_name"),
            "category",
            "latitude",
            "longitude",
        )
    )

    df_silver = (
        df_orders_clean.join(df_restos_clean, on="restaurant_id", how="left")
        .withColumn(
            "distance_band",
            F.when(F.col("distance_km") < 2.0, "court")
            .when(F.col("distance_km") <= 5.0, "moyen")
            .otherwise("long"),
        )
        .withColumn(
            "revenue_per_km",
            F.when(
                F.col("distance_km") > 0,
                F.round(F.col("amount_mad") / F.col("distance_km"), 2),
            ).otherwise(F.lit(None).cast(DoubleType())),
        )
        .withColumn("silver_processed_at", F.current_timestamp())
    )

    df_silver.write.mode("overwrite").partitionBy("city").parquet(
        "/workspace/lake/silver/orders_enriched"
    )
    print(f"Silver orders_enriched count : {df_silver.count()}")

    # =========================================================================
    # 3. COUCHE GOLD : KPIs métier et tables analytiques
    # =========================================================================
    print("--- [GOLD] Calcul des agrégations métier ---")
    silver_read = spark.read.parquet("/workspace/lake/silver/orders_enriched")

    # 3a. KPIs par ville et statut/bande de distance
    df_gold_city = (
        silver_read.groupBy("city", "distance_band")
        .agg(
            F.count("*").alias("nb_commandes"),
            F.sum(F.when(F.col("status") == "DELIVERED", 1).otherwise(0)).alias(
                "nb_livrees"
            ),
            F.round(
                F.sum(
                    F.when(
                        F.col("status") == "DELIVERED", F.col("amount_mad")
                    ).otherwise(0.0)
                ),
                2,
            ).alias("ca_delivre_mad"),
            F.round(
                F.avg(
                    F.when(F.col("status") == "DELIVERED", F.col("amount_mad"))
                ),
                2,
            ).alias("panier_moyen_mad"),
            F.round(F.avg("distance_km"), 2).alias("distance_moy_km"),
        )
        .withColumn(
            "taux_livraison_pct",
            F.when(
                F.col("nb_commandes") > 0,
                F.round((F.col("nb_livrees") / F.col("nb_commandes")) * 100, 2),
            ).otherwise(0.0),
        )
        .orderBy("city", F.desc("ca_delivre_mad"))
    )

    df_gold_city.write.mode("overwrite").parquet(
        "/workspace/lake/gold/city_kpis"
    )
    print("=== Gold : KPIs par ville et distance_band ===")
    df_gold_city.show(30, truncate=False)

    # 3b. Classement des restaurants (CA délivré)
    df_gold_restos = (
        silver_read.filter(F.col("status") == "DELIVERED")
        .groupBy("city", "restaurant_id", "restaurant_name", "category")
        .agg(
            F.count("*").alias("nb_livraisons"),
            F.round(F.sum("amount_mad"), 2).alias("ca_mad"),
            F.round(F.avg("amount_mad"), 2).alias("panier_moyen_mad"),
        )
        .orderBy(F.desc("ca_mad"))
    )

    df_gold_restos.write.mode("overwrite").parquet(
        "/workspace/lake/gold/restaurant_performance"
    )
    print("=== Gold : Top 15 restaurants ===")
    df_gold_restos.show(15, truncate=False)

    # 3c. Intégration optionnelle des sorties de streaming (Partie 4)
    streaming_path = "/workspace/lake/streaming/orders_windowed"
    if os.path.exists(streaming_path):
        try:
            df_stream = spark.read.parquet(streaming_path)
            stream_count = df_stream.count()
            print(
                f"=== Streaming (Part 4) détecté : {stream_count} lignes"
                " agrégées ==="
            )
            df_stream.show(10, truncate=False)
        except Exception as exc:
            print(f"Note lecture streaming : {exc}")

    print(
        f"Pipeline Partie 5 terminé avec succès en {time.time() - t_start:.2f}"
        " s."
    )
    spark.stop()


if __name__ == "__main__":
    main()