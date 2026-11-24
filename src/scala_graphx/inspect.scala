val df = spark.read.parquet("/workspace/lake/gold/graph_analysis/graphx_metrics")
df.printSchema()
df.show(false)
sys.exit(0)
