from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("Part1-Cluster-Check")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

sc = spark.sparkContext
print("=== INFOS CLUSTER SPARK ===")
print("Version Spark :", spark.version)
print("Master        :", sc.master)
print("AppName       :", sc.appName)
print("Parallelism   :", sc.defaultParallelism)

spark.stop()