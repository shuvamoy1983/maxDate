 package Streamsets

/**
 * Hello world!
 *
 */

import com.streamsets.pipeline.api.Record
import com.streamsets.pipeline.spark.api.SparkTransformer
import com.streamsets.pipeline.spark.api.TransformResult
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext
import java.io.Serializable
import java.util

import com.streamsets.pipeline.api.Record
import com.streamsets.pipeline.spark.api.SparkTransformer
import com.streamsets.pipeline.spark.api.TransformResult
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext
import java.io.Serializable
import java.util

import org.apache.spark.sql.SparkSession

case class emp(id: String,name: String,Sal:String)

class CustomTransformer extends SparkTransformer with Serializable {
  var emptyRDD: JavaRDD[(Record, String)] = _

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("AppName")
      .config("spark.master", "local")
      .getOrCreate()


  import spark.sqlContext.implicits._

  override def init(javaSparkContextInstance: JavaSparkContext, params: util.List[String]): Unit = {
    // Create an empty JavaPairRDD to return as 'errors'
    emptyRDD = javaSparkContextInstance.emptyRDD
  }

  override def transform(recordRDD: JavaRDD[Record]): Unit = {
    val rdd = recordRDD.rdd


    val errors = emptyRDD

    // Apply a function to the incoming records
    val result = rdd.map(p=> emp(p(0),p(1),p(2))).toDF()

    result.write.mode("append").parquet("hdfs://localhost:9000/data/parquet")

    // return result
    //new TransformResult(result.toJavaRDD(), new JavaPairRDD[Record, String](errors))
  }
}
