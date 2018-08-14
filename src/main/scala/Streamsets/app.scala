package Streamsets

import org.apache.spark.sql.SparkSession

object app {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession =
      SparkSession
        .builder()
        .appName("AppName")
        .config("spark.master", "local")
        .getOrCreate()


    val p=spark.read.parquet("hdfs://localhost:9000/data/parquet/par-3986aa60-9d63-11e8-9eaa-631081204dcf_1c4474ec-dc78-4bb1-bc5c-efff857d3864")
    p.show(2)
  }

}
