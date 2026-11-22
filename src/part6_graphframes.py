import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    t0 = time.time()

    spark = (
        SparkSession.builder.appName("Part6-Graph-Analytics")
        .master("spark://spark-master:7077")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setCheckpointDir("/workspace/lake/checkpoints/graphframes")

    silver_path = "/workspace/lake/silver/orders_enriched"
    df_orders = spark.read.parquet(silver_path).filter(
        F.col("restaurant_id").isNotNull() & F.col("city").isNotNull()
    )

    # 1. Modèle du graphe : sommets (restaurants, villes, livreurs)
    v_restos = (
        df_orders.select(
            F.col("restaurant_id").alias("id"),
            F.lit("restaurant").alias("node_type"),
            F.coalesce(F.col("restaurant_name"), F.col("restaurant_id")).alias(
                "label"
            ),
            F.col("city"),
        )
        .filter(F.col("id").isNotNull())
        .dropDuplicates(["id"])
    )

    v_cities = (
        df_orders.select(
            F.concat(F.lit("CITY_"), F.col("city")).alias("id"),
            F.lit("city").alias("node_type"),
            F.col("city").alias("label"),
            F.col("city"),
        )
        .filter(F.col("city").isNotNull())
        .dropDuplicates(["id"])
    )

    v_couriers = (
        df_orders.filter(F.col("courier_id").isNotNull())
        .select(
            F.col("courier_id").alias("id"),
            F.lit("courier").alias("node_type"),
            F.col("courier_id").alias("label"),
            F.col("city"),
        )
        .dropDuplicates(["id"])
    )

    vertices = (
        v_restos.unionByName(v_cities)
        .unionByName(v_couriers)
        .dropDuplicates(["id"])
    )

    # 2. Arêtes : Restaurant -> Ville et Livreur -> Ville
    edges_restos_city = (
        df_orders.groupBy(
            F.col("restaurant_id").alias("src"),
            F.concat(F.lit("CITY_"), F.col("city")).alias("dst"),
        )
        .agg(
            F.lit("delivers_in").alias("relationship"),
            F.count("*").alias("nb_orders"),
            F.round(F.sum("amount_mad"), 2).alias("total_amount_mad"),
            F.round(F.avg("distance_km"), 2).alias("avg_distance_km"),
        )
        .filter(F.col("src").isNotNull() & F.col("dst").isNotNull())
    )

    edges_courier_city = (
        df_orders.filter(F.col("courier_id").isNotNull())
        .groupBy(
            F.col("courier_id").alias("src"),
            F.concat(F.lit("CITY_"), F.col("city")).alias("dst"),
        )
        .agg(
            F.lit("operates_in").alias("relationship"),
            F.count("*").alias("nb_orders"),
            F.round(F.sum("amount_mad"), 2).alias("total_amount_mad"),
            F.round(F.avg("distance_km"), 2).alias("avg_distance_km"),
        )
    )

    edges = edges_restos_city.unionByName(edges_courier_city)

    # 3. Contrôle des relations inter-tenant / ville
    v_info = vertices.select(
        F.col("id").alias("v_id"), F.col("city").alias("v_city")
    )

    df_cross_tenant = (
        edges.join(v_info, edges["src"] == v_info["v_id"], "left")
        .withColumnRenamed("v_city", "src_city")
        .drop("v_id")
        .join(v_info, edges["dst"] == v_info["v_id"], "left")
        .withColumnRenamed("v_city", "dst_city")
        .drop("v_id")
        .withColumn(
            "is_cross_city",
            F.when(F.col("src_city") != F.col("dst_city"), F.lit(True)).otherwise(
                F.lit(False)
            ),
        )
    )

    print(
        "Relations hors même ville :",
        df_cross_tenant.filter(F.col("is_cross_city") == True).count(),
    )

    # 4. Comparaison des degrés
    in_deg = (
        edges.groupBy("dst")
        .agg(
            F.sum("nb_orders").alias("in_weighted_orders"),
            F.count("*").alias("in_degree"),
        )
        .withColumnRenamed("dst", "id")
    )

    out_deg = (
        edges.groupBy("src")
        .agg(
            F.sum("nb_orders").alias("out_weighted_orders"),
            F.count("*").alias("out_degree"),
        )
        .withColumnRenamed("src", "id")
    )

    df_degrees = (
        vertices.join(in_deg, on="id", how="left")
        .join(out_deg, on="id", how="left")
        .fillna(
            0,
            subset=[
                "in_weighted_orders",
                "in_degree",
                "out_weighted_orders",
                "out_degree",
            ],
        )
        .withColumn("total_degree", F.col("in_degree") + F.col("out_degree"))
        .orderBy(F.desc("out_weighted_orders"), F.desc("in_weighted_orders"))
    )

    print("=== Comparaison des degrés (Top 15) ===")
    df_degrees.show(15, truncate=False)

    # 5. Motif GraphFrames + Connected Components
    df_cc = None
    try:
        from graphframes import GraphFrame

        g = GraphFrame(vertices, edges)

        df_motifs = (
            g.find("(a)-[e]->(b)")
            .filter("e.nb_orders >= 250")
            .select(
                F.col("a.id").alias("src"),
                F.col("b.id").alias("dst"),
                F.col("e.relationship").alias("relationship"),
                F.col("e.nb_orders").alias("nb_orders"),
            )
            .orderBy(F.desc("nb_orders"))
        )
        print("=== Motif GraphFrames (nb_orders >= 250) ===")
        df_motifs.show(10, truncate=False)

        # Export immédiat des motifs
        df_motifs.write.mode("overwrite").parquet(
            "/workspace/lake/gold/graph_motifs"
        )

        # Algorithme léger : Connected Components
        cc = g.connectedComponents()
        df_cc = (
            cc.select(
                "id",
                "node_type",
                "label",
                "city",
                F.col("component").alias("component_id"),
            )
            .orderBy("component_id", "id")
        )
        print("=== Top 10 Connected Components ===")
        df_cc.show(10, truncate=False)
    except Exception as e:
        print("GraphFrame non exécuté :", e)

    # 6. Exports Parquet dans /workspace/lake/gold/
    df_degrees.write.mode("overwrite").parquet("/workspace/lake/gold/graph_nodes")
    edges.write.mode("overwrite").parquet("/workspace/lake/gold/graph_edges")
    df_cross_tenant.write.mode("overwrite").parquet(
        "/workspace/lake/gold/graph_cross_tenant"
    )
    if df_cc is not None:
        df_cc.write.mode("overwrite").parquet(
            "/workspace/lake/gold/graph_connected_components"
        )

    print(f"Partie 6 terminée en {time.time() - t0:.2f} s.")
    spark.stop()


if __name__ == "__main__":
    main()