import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    t0 = time.time()

    spark = (
        SparkSession.builder.appName("Part6-Connected-Components")
        .master("spark://spark-master:7077")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    nodes = spark.read.parquet("/workspace/lake/gold/graph_nodes")
    edges = spark.read.parquet("/workspace/lake/gold/graph_edges")

    # Identifiant de composante basé sur la ville associée au nœud ou à ses arêtes
    src_city = edges.select(F.col("src").alias("nid"), F.col("dst").alias("city_id"))
    dst_city = edges.select(F.col("dst").alias("nid"), F.col("dst").alias("city_id"))

    node_comp = (
        src_city.unionByName(dst_city)
        .filter(F.col("city_id").startswith("CITY_"))
        .groupBy("nid")
        .agg(F.min("city_id").alias("component_id"))
    )

    df_cc = (
        nodes.join(node_comp, nodes["id"] == node_comp["nid"], "left")
        .drop("nid")
        .withColumn(
            "component_id",
            F.coalesce(
                F.col("component_id"),
                F.when(F.col("city").isNotNull(), F.concat(F.lit("CITY_"), F.col("city"))).otherwise(F.col("id")),
            ),
        )
        .select("id", "node_type", "label", "city", "component_id")
        .orderBy("component_id", "node_type", "id")
    )

    print("=== Aperçu Connected Components ===")
    df_cc.show(15, truncate=False)

    df_cc.write.mode("overwrite").parquet("/workspace/lake/gold/graph_connected_components")
    print(f"graph_connected_components écrit en {time.time() - t0:.2f} s.")
    spark.stop()


if __name__ == "__main__":
    main()
