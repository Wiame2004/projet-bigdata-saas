import json
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("Part2-RDD-Batch")
    .master("spark://spark-master:7077")
    .getOrCreate()
)
sc = spark.sparkContext

# 1. Lecture en RDD et parsing JSON
raw_rdd = sc.textFile("/workspace/data/orders_history.jsonl")
parsed_rdd = raw_rdd.map(lambda line: json.loads(line))

# 2. Garder uniquement les commandes DELIVERED
delivered_rdd = parsed_rdd.filter(lambda row: row.get("status") == "DELIVERED")

# 3. Chiffre d'affaires total par ville (map + reduceByKey)
ca_by_city_rdd = delivered_rdd.map(
    lambda row: (row["city"], float(row["amount_mad"]))
).reduceByKey(lambda a, b: a + b)

# 4. Panier moyen par ville en un seul passage (aggregateByKey : (somme, compte))
zero_val = (0.0, 0)
seq_op = lambda acc, val: (acc[0] + val, acc[1] + 1)
comb_op = lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])

avg_basket_by_city_rdd = (
    delivered_rdd.map(lambda row: (row["city"], float(row["amount_mad"])))
    .aggregateByKey(zero_val, seq_op, comb_op)
    .mapValues(lambda s_c: round(s_c[0] / s_c[1], 2) if s_c[1] > 0 else 0.0)
)

# 5. Top 5 des restaurants par nombre de commandes livrées
top5_restaurants = (
    delivered_rdd.map(lambda row: (row["restaurant_id"], 1))
    .reduceByKey(lambda a, b: a + b)
    .takeOrdered(5, key=lambda x: -x[1])
)

# Actions (déclenchent l'exécution du DAG)
ca_results = ca_by_city_rdd.collect()
avg_results = dict(avg_basket_by_city_rdd.collect())

print("\n=== CHIFFRE D'AFFAIRES ET PANIER MOYEN PAR VILLE (DELIVERED) ===")
for city, ca in sorted(ca_results, key=lambda x: -x[1]):
  print(f"Ville: {city:15s} | CA: {ca:10.2f} MAD | Panier moyen: {avg_results.get(city, 0.0):6.2f} MAD")

print("\n=== TOP 5 RESTAURANTS (COMMANDES DELIVERED) ===")
for resto_id, count in top5_restaurants:
  print(f"Restaurant: {resto_id:10s} | Commandes livrées: {count}")

# 6. Lineage (toDebugString)
print("\n=== LINEAGE (toDebugString) ===")
print(avg_basket_by_city_rdd.toDebugString().decode("utf-8"))

spark.stop()