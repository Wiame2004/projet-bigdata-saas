from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import glob, os

spark = (
    SparkSession.builder
    .appName("ExplainPart3")
    .master("local[*]")
    .config("spark.driver.extraJavaOptions", "-Dlog4j.rootCategory=ERROR,console")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# Recherche automatique des données disponibles dans /workspace/lake
candidates = glob.glob("/workspace/lake/**/*", recursive=True)
parquet_dirs = sorted({os.path.dirname(p) for p in candidates if p.endswith(".parquet")})
json_files = sorted([p for p in candidates if p.endswith(".json")])

if parquet_dirs:
    path = parquet_dirs[0]
    df = spark.read.parquet(path)
elif json_files:
    path = os.path.dirname(json_files[0])
    df = spark.read.json(path)
else:
    # Jeu de données de secours si aucun fichier n'est trouvé sur le chemin
    df = spark.createDataFrame([
        ("t1", "Casablanca", "Burger", 120.0, 2.5),
        ("t1", "Rabat", "Pizza", 95.0, 6.0),
        ("t2", "Kenitra", "Tacos", 75.0, 12.0),
        ("t2", "Tanger", "Sushi", 180.0, 4.0),
    ], ["tenant_id", "city", "category", "amount", "distance_km"])
    path = "fallback_sample"

df_band = df.withColumn(
    "distance_band",
    F.when(F.col("distance_km") < 3, "0-3km")
     .when(F.col("distance_km") <= 8, "3-8km")
     .otherwise(">8km")
) if "distance_km" in df.columns else df

num_col = "amount" if "amount" in df.columns else [c for c, t in df.dtypes if t in ("int", "bigint", "double", "float")][0]
grp_cols = [c for c in ["city", "category", "distance_band"] if c in df_band.columns] or [df_band.columns[0]]

agg_df = df_band.groupBy(*grp_cols).agg(
    F.count("*").alias("nb_orders"),
    F.round(F.sum(F.col(num_col).cast("double")), 2).alias("total_ca")
)

print("=== SOURCE:", path, "===")
import io, contextlib
buf = io.StringIO()
with contextlib.redirect_stdout(buf):
    agg_df.explain(True)

lines = buf.getvalue().splitlines()
keywords = ("Parsed Logical Plan", "Analyzed Logical Plan", "Optimized Logical Plan", "Physical Plan", "Aggregate", "HashAggregate", "SortAggregate", "BroadcastHashJoin", "SortMergeJoin", "Filter", "Project", "CaseWhen")
filtered = [ln for ln in lines if any(k in ln for k in keywords) or ln.startswith("==")]
print("\n".join(filtered[:35]))

print("\n=== RESULTATS PARTIE 3 ===")
agg_df.show(10, truncate=False)
spark.stop()