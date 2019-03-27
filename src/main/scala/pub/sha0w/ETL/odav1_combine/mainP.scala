package pub.sha0w.ETL.odav1_combine

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import utils.{JedisImplSer, UUIDEvaluator}

object mainP {
  val baseFileLocation : String = File.pathSeparator + "oadv2"
  val id_field_name = "id"
  val mag_paper : String = "/oadv1/mag/papers/*"
  val uuid: UUIDEvaluator = UUIDEvaluator.getInstance()

  val aminer_paper : String = "/oadv1/aminer/papers/*"


  val paper_temp_out_path =  "/tmp/oadv1/paper_combined"

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
  def main(args: Array[String]): Unit = {
    import sys.process._
    val t3 = s"hadoop fs -rmr $paper_temp_out_path"!

    val sc = sparkSession.sparkContext
    sc.setCheckpointDir("/tmp/oadv2/checkpoint_location")
    val sql = sparkSession.sqlContext
    val jedis : JedisImplSer = new JedisImplSer("10.0.88.50", 6379)
    val jedis_broad : Broadcast[JedisImplSer] = sc.broadcast(jedis)
    lazy val mag_paper_rdd = sc.textFile(mag_paper).map(parse(_).asInstanceOf[JObject])

    lazy val aminer_paper_rdd =  sc.textFile(aminer_paper).map(parse(_).asInstanceOf[JObject])

    val process_fun: (RDD[JObject], RDD[JObject], String, String) => Unit = process(sql, jedis_broad)
//    val process_fun_big: (String, RDD[JObject], RDD[JObject], String) => Unit = process(sql)
    process_fun(mag_paper_rdd, aminer_paper_rdd, "oadv1.papers", paper_temp_out_path)
  }

  def process(sql : SQLContext, jedis: Broadcast[JedisImplSer])
             (mag_rdd : RDD[JObject], aminer_rdd : RDD[JObject]
             , saveName : String, temp_location : String): Unit = {
    val final_venues = linkMerge(jedis.value
      ,mag_rdd
      ,aminer_rdd)
    final_venues.saveAsTextFile(temp_location)
    val final_venues_df = sql.read.json(temp_location)
    final_venues_df.write.mode(SaveMode.Overwrite).saveAsTable(saveName)
  }



  def linkMerge(jedis : JedisImplSer,
                part_a : RDD[JObject],
                part_b : RDD[JObject]) :  RDD[String] = {
    // m -> a
    val kv_a = part_a
      .map(j => (j.values("id").asInstanceOf[String], j))
    println(kv_a.take(1).head)
    val kv_a_joined = kv_a.map(pair => {
      (Option(jedis.getJedis.get(pair._1)).getOrElse(pair._1), (pair._2, "a"))
    })
    kv_a_joined.checkpoint()
    val kv_b = part_b
      .map(j => (j.values("id").asInstanceOf[String], (j, "b")))
    println(kv_b.take(1).head)
    kv_b.checkpoint()

    (kv_a_joined union kv_b)
      .repartition(500)
      .reduceByKey((a, b) => {
        if (a._2 == "a")
          (b._1 merge a._1, "a")
        else if (b._2 == "a")
          (a._1 merge b._1, "a")
        else
          (a._1 merge b._1,"b")
      }).values.map(_._1).map(j => compact(j))
  }

}