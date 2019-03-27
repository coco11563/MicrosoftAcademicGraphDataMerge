package pub.sha0w.ETL.odav1_combine

import com.redislabs.provider.redis._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import pub.sha0w.ETL.JedisUtils
import utils.JedisImplSer

object initRedis {
  val paper_linking : String = "/oadv1/linking_relations.txt"
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
      .set("spark.driver.maxResultSize", "4g")
      .set("spark.default.parallelism", "500")
      .set("spark.redis.host", "10.0.88.50")
      .set("spark.redis.port", "6379")
      .setAppName("MY_APP_NAME")
    val sparkSession: SparkSession = SparkSession
      .builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()
    val sc = sparkSession.sparkContext
    sc.setCheckpointDir("/tmp/oadv2/checkpoint_location")
    val sql = sparkSession.sqlContext
    val link_ds = sql.read.json(paper_linking)
    val mid_index = link_ds.schema.fieldIndex("mid")
    val aid_index = link_ds.schema.fieldIndex("aid")
    val link = link_ds
      .rdd
      .map(r => (r(aid_index).asInstanceOf[String], r(mid_index).asInstanceOf[String]))
    sql.sparkSession.sparkContext.toRedisKV(link)
    sparkSession.stop()
  }
}
