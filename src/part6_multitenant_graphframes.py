from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from graphframes import GraphFrame

LAKE_PATH = "/workspace/lake"  # à adapter si votre dossier lake est monté ailleurs


def build_multitenant_graph_data(spark: SparkSession):
    tenants = ["tenant_alpha", "tenant_beta", "tenant_gamma"]
    vertices_rows = []
    edges_rows = []

    for t in tenants:
        # Sommets : users, roles, resources (identifiants scopés par tenant pour éviter toute collision)
        users = [
            (f"{t}_U1", t, "user", f"alice@{t}"),
            (f"{t}_U2", t, "user", f"bob@{t}"),
            (f"{t}_U3", t, "user", f"charlie@{t}"),
        ]
        roles = [
            (f"{t}_R_ADMIN", t, "role", "Admin"),
            (f"{t}_R_DEV", t, "role", "Developer"),
            (f"{t}_R_VIEW", t, "role", "Viewer"),
        ]
        resources = [
            (f"{t}_RES_DB", t, "resource", "prod_db"),
            (f"{t}_RES_BILLING", t, "resource", "billing_api"),
            (f"{t}_RES_LOGS", t, "resource", "audit_logs"),
        ]
        vertices_rows.extend(users + roles + resources)

        # Arêtes : HAS_ROLE, CAN_ACCESS, ACCESSED
        edges_rows.extend([
            (f"{t}_U1", f"{t}_R_ADMIN", t, t, "HAS_ROLE", 1.0),
            (f"{t}_U1", f"{t}_R_DEV", t, t, "HAS_ROLE", 1.0),
            (f"{t}_U2", f"{t}_R_DEV", t, t, "HAS_ROLE", 1.0),
            (f"{t}_U3", f"{t}_R_VIEW", t, t, "HAS_ROLE", 1.0),
            (f"{t}_R_ADMIN", f"{t}_RES_DB", t, t, "CAN_ACCESS", 5.0),
            (f"{t}_R_ADMIN", f"{t}_RES_BILLING", t, t, "CAN_ACCESS", 4.0),
            (f"{t}_R_DEV", f"{t}_RES_DB", t, t, "CAN_ACCESS", 3.0),
            (f"{t}_R_VIEW", f"{t}_RES_LOGS", t, t, "CAN_ACCESS", 2.0),
            (f"{t}_U1", f"{t}_RES_BILLING", t, t, "ACCESSED", 12.0),
        ])

    # Exemple d'arête inter-tenant pour tester le contrôle d'isolation et la détection d'anomalies
    edges_rows.append((
        "tenant_alpha_U2",
        "tenant_beta_RES_DB",
        "tenant_alpha",
        "tenant_beta",
        "ACCESSED",
        2.0,
    ))

    vertices_df = spark.createDataFrame(
        vertices_rows, ["id", "tenant_id", "node_type", "label"]
    )
    edges_df = (
        spark.createDataFrame(
            edges_rows,
            ["src", "dst", "src_tenant_id", "dst_tenant_id", "relation", "weight"],
        )
        .withColumn("tenant_id", F.col("src_tenant_id"))
    )
    return vertices_df, edges_df


def main():
    spark = (
        SparkSession.builder
        .appName("Part6-MultiTenant-GraphFrames-Prep")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    vertices_df, edges_df = build_multitenant_graph_data(spark)
    gf = GraphFrame(vertices_df, edges_df)

    print("=== Sommets par tenant et type ===")
    gf.vertices.groupBy("tenant_id", "node_type").count().show(truncate=False)

    print("=== Arêtes par tenant et relation ===")
    gf.edges.groupBy("tenant_id", "relation").count().show(truncate=False)

    print("=== Contrôle relations inter-tenant ===")
    cross_tenant = gf.edges.filter(F.col("src_tenant_id") != F.col("dst_tenant_id"))
    cross_tenant.show(truncate=False)

    print("=== Motifs utilisateur -> rôle -> ressource ===")
    motifs = (
        gf.find("(u)-[er]->(r); (r)-[ea]->(res)")
        .filter(
            (F.col("u.node_type") == "user")
            & (F.col("r.node_type") == "role")
            & (F.col("res.node_type") == "resource")
            & (F.col("er.relation") == "HAS_ROLE")
            & (F.col("ea.relation") == "CAN_ACCESS")
        )
        .select(
            F.col("u.tenant_id").alias("tenant_id"),
            F.col("u.id").alias("user_id"),
            F.col("r.id").alias("role_id"),
            F.col("res.id").alias("resource_id"),
        )
    )
    motifs.show(truncate=False)

    # Export Parquet attendu par TenantGraphX.scala et le rapport
    out_base = f"{LAKE_PATH}/gold/graph_analysis"
    vertices_df.write.mode("overwrite").parquet(f"{out_base}/vertices")
    edges_df.write.mode("overwrite").parquet(f"{out_base}/edges")
    motifs.write.mode("overwrite").parquet(f"{out_base}/motifs")
    cross_tenant.write.mode("overwrite").parquet(f"{out_base}/cross_tenant")

    print(f"Tables écrites dans {out_base}/vertices et {out_base}/edges")
    spark.stop()


if __name__ == "__main__":
    main()