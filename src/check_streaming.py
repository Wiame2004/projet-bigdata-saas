from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("Check-Streaming-Parquet")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

df = spark.read.parquet("/workspace/lake/streaming/orders_windowed")
print("Nombre de lignes agrégées :", df.count())
df.show(20, truncate=False)
spark.stop()