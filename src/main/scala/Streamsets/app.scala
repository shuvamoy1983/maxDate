package Streamsets

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, TimestampType}

case class emp(id: String,sal: String,time_stamp : String)

object app {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession =
      SparkSession
        .builder()
        .appName("AppName")
        .config("spark.master", "local")
        .getOrCreate()


    import spark.sqlContext.implicits._
    val p =spark.sparkContext.textFile("hdfs://localhost:9000/input/t2.txt").map(p=> p.split(","))
    val df=p.map(p=> emp(p(0),p(1),p(2))).toDF()

    val df1=df.withColumn("eventTime", unix_timestamp(col("time_stamp"), "yyyy-MM-dd").cast(TimestampType).cast(DateType))
    df1.show()

    //df1.select(""" *, year(col("eventTime")) as date_year, month(col("eventTime")) as date_month""").show()

    df1.select('*, year('eventTime) as 'date_year, month('eventTime) as 'date_month).write.partitionBy("date_year","date_month").mode("overwrite")
      .format("parquet").save("hdfs://localhost:9000/output/par1")

    spark.read.parquet("hdfs://localhost:9000/output/par1/").printSchema()


    //df1.select(""" *, year("eventTime") as date_year, month("eventTime") as date_month""")
      //.write.partitionBy("date_year","date_month").mode("overwrite").format("parquet").save("hdfs://localhost:9000/output/par")


   // df1.write.partitionBy("eventTime",).format("parquet").save("hdfs://localhost:9000/output/")

    // spark.sql("""select id,sal,time_stamp from test""").show()


    //df.write.partitionBy("time_stamp").format("parquet").save("hdfs://localhost:9000/input/")
   // val p=spark.read.parquet("hdfs://localhost:9000/data/parquet/par-3986aa60-9d63-11e8-9eaa-631081204dcf_1c4474ec-dc78-4bb1-bc5c-efff857d3864")
    //p.show(2)
  }

}
