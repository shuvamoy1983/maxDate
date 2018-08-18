package Streamsets

import org.apache.spark.sql.SparkSession

object app1 {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession =
      SparkSession
        .builder()
        .appName("AppName")
        .config("spark.master", "local")
        .getOrCreate()

   /* +---+---+----------+----------+---------+----------+
    | id|sal|time_stamp| eventTime|date_year|date_month|
    +---+---+----------+----------+---------+----------+
    |  2|200|2017-09-12|2017-09-12|     2017|         9|
      |  4|400|2017-08-12|2017-08-12|     2017|         8|
      |  1|100|2017-10-13|2017-10-13|     2017|        10|
      |  3|300|2018-09-09|2018-09-09|     2018|         9|
      |  4|400|2016-08-12|2016-08-12|     2016|         8|
      |  3|300|2019-09-09|2019-09-09|     2019|         9|
      |  2|200|2018-09-12|2018-09-12|     2018|         9|
      |  1|100|2018-10-13|2018-10-13|     2018|        10|
      +---+---+----------+----------+---------+----------+ */

    /* +---+---+----------+
+---+---+----------+
| id|sal| eventTime|
+---+---+----------+
|  3|300|2019-09-09|
|  1|100|2018-10-13|
|  4|400|2017-08-12|
|  2|200|2018-09-12|
+---+---+----------+ */



   val  df = spark.read.parquet("hdfs://localhost:9000/output/par/")
    val  df1 = spark.read.parquet("hdfs://localhost:9000/output/par1/" )



    val df_join=df.union(df1)
    df_join.show()
    df_join.createOrReplaceTempView("test")
    spark.sql("select id,sal,time_stamp,eventTime from ( " +
      "        select id,sal,time_stamp,eventTime, row_number() over(partition by id order by eventTime desc ) " +
      "r from test) where r=1 ").show()

    //val p=spark.sql("select tm.id,sal,tm.max_dt,eventTime from test t inner join (select id, max(time_stamp) as max_dt " +
    //  "from test t2 group by id) tm on t.id=tm.id and t.time_stamp=tm.max_dt")

  // p.show()
    //19:57:01, 19:57:49
    //21:13:26, 21:13:45
    // 21:14:39  21:15:01


  }
  }
