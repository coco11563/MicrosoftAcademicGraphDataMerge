package pub.sha0w.ETL.odav1_combine

import com.redislabs.provider.redis._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import pub.sha0w.ETL.JedisUtils
import utils.JedisImplSer

object initRedis {
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
    val link_ds = sql.read.json(args(0))
    val mid_index = link_ds.schema.fieldIndex("mid")
    val aid_index = link_ds.schema.fieldIndex("aid")
    val link = link_ds
      .rdd
      .map(r => (r(mid_index).asInstanceOf[String], r(aid_index).asInstanceOf[String]))
    if (args.length < 2 || args(1) == "overwrite") {
      val jedis : JedisImplSer = new JedisImplSer("10.0.88.50", 6379)
      JedisUtils.resetRedis(jedis.getJedis)
    }
    sql.sparkSession.sparkContext.toRedisKV(link)
    sparkSession.stop()
//    if (saveName.contains("author")) {
//      assert(jedis.value.getJedis.get("2074947230") == "53f4651cdabfaeee22a4ed98")
//      println("inited done")
//    } else if (saveName.contains("venues")) {
//      assert(jedis.value.getJedis.get("5bf573b81c5a1dcdd96ec669") == "5451a5c9e0cf0b02b5f3bd3c")
//      println("inited done")
//    } else if (saveName.contains("papers")) {
//      assert(jedis.value.getJedis.get("2164199381") == "53e99784b7602d9701f3e141")
//      println("inited done")
//    } else {
//      println("inited wrong")
//    }
  }
}
