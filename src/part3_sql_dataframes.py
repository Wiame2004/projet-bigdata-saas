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

spark = (
    SparkSession.builder.appName("Part3-SparkSQL-DataFrames")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

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

df_orders = spark.read.schema(orders_schema).json(
    "/workspace/data/orders_history.jsonl"
)
df_restos = (
    spark.read.schema(restaurants_schema)
    .option("multiline", True)
    .json("/workspace/data/restaurants.json")
)

# 1. CA par ville (DataFrame API puis Spark SQL)
t0 = time.time()
df_ca_city = (
    df_orders.filter(F.col("status") == "DELIVERED")
    .groupBy("city")
    .agg(
        F.round(F.sum("amount_mad"), 2).alias("ca_total"),
        F.round(F.avg("amount_mad"), 2).alias("panier_moyen"),
        F.count("*").alias("nb_commandes"),
    )
    .orderBy(F.desc("ca_total"))
)
df_ca_city.show(truncate=False)
print(f"Temps DataFrame : {time.time() - t0:.2f} s")

df_orders.createOrReplaceTempView("orders_view")
spark.sql("""
    SELECT city,
           ROUND(SUM(amount_mad), 2) AS ca_total,
           ROUND(AVG(amount_mad), 2) AS panier_moyen,
           COUNT(*) AS nb_commandes
    FROM orders_view
    WHERE status = 'DELIVERED'
    GROUP BY city
    ORDER BY ca_total DESC
""").show(truncate=False)

# 2. Jointure avec restaurants + CA par ville et catégorie
df_joined = (
    df_orders.filter(F.col("status") == "DELIVERED")
    .join(
        df_restos.select("restaurant_id", "name", "category"),
        on="restaurant_id",
        how="inner",
    )
    .withColumn(
        "distance_band",
        F.when(F.col("distance_km") < 2.0, "court")
        .when(F.col("distance_km") <= 5.0, "moyen")
        .otherwise("long"),
    )
)

df_joined.groupBy("city", "category").agg(
    F.round(F.sum("amount_mad"), 2).alias("ca_mad"),
    F.count("*").alias("nb_livraisons"),
).orderBy("city", F.desc("ca_mad")).show(25, truncate=False)

# 3. Comparaison UDF vs when/otherwise
distance_udf = F.udf(
    lambda d: (
        None
        if d is None
        else ("court" if d < 2.0 else ("moyen" if d <= 5.0 else "long"))
    ),
    StringType(),
)

t1 = time.time()
df_orders.withColumn("band_udf", distance_udf(F.col("distance_km"))).groupBy(
    "band_udf"
).count().show()
print(f"Temps UDF : {time.time() - t1:.2f} s")

t2 = time.time()
df_orders.withColumn(
    "band_nat",
    F.when(F.col("distance_km") < 2.0, "court")
    .when(F.col("distance_km") <= 5.0, "moyen")
    .otherwise("long"),
).groupBy("band_nat").count().show()
print(f"Temps when/otherwise : {time.time() - t2:.2f} s")

# 4. Écriture Parquet partitionnée par city
df_joined.write.mode("overwrite").partitionBy("city").parquet(
    "/workspace/lake/silver/orders_history"
)
print("Parquet écrit dans /workspace/lake/silver/orders_history")

# 5. Plan d'exécution explain(True)
df_joined.explain(True)

spark.stop()