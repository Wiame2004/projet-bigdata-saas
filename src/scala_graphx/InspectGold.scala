import org.apache.spark.sql.SparkSession
object InspectGold {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("InspectGoldMetrics")
      .master("local[*]")
      .config("spark.driver.extraJavaOptions", "-Dlog4j.rootCategory=ERROR,console")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.read.parquet("/workspace/lake/gold/graph_analysis/graphx_metrics")
    df.printSchema()
    df.show(15, false)

    spark.stop()
  }
}