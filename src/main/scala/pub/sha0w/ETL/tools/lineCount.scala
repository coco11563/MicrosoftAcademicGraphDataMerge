package pub.sha0w.ETL.tools

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object lineCount {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
      .set("spark.driver.maxResultSize", "6g")
      .setAppName("MY_APP_NAME")

    val sparkSession: SparkSession = SparkSession
      .builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()
    val sc = sparkSession.sparkContext

    val tables = sparkSession.read.table(args(0))
    val rdd = tables.select(args(1), args(2)).rdd
    rdd
      .map(r => (r.getAs[String](args(1)), r.getAs[String](args(2))))
      .filter(a => {a._2 != null})
      .map(a => {
        (a._1, a._2.length)
      })
      .sortBy(a => a._2)
      .saveAsTextFile("/tmp/"+ args(0) + ".out.status")
  }
}
