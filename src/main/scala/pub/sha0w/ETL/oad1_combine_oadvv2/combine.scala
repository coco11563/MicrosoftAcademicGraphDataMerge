package pub.sha0w.ETL.oad1_combine_oadvv2

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.json4s
import org.json4s._
import org.json4s.jackson.JsonMethods._
import utils.JedisImplSer

object combine {
  def main(args: Array[String]): Unit = {
    val paper_temp_out_path_v1 =  "/tmp/oadv1/paper_combined_af"
    val paper_temp_out_path_v2 =  "/tmp/oadv2/paper_combined_af"
    val out_path = "/tmp/oadv2/v1_v2_combined_papers"
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
    val jedis : JedisImplSer = new JedisImplSer("10.0.88.50", 6379)
    val sc = sparkSession.sparkContext
    sc.setCheckpointDir("/tmp/oadv2/checkpoint_location")
    sparkSession.read.table("oadv2.merged_table").write.json(paper_temp_out_path_v2)

    val v1_paper_rdd = sc.textFile(paper_temp_out_path_v1).map(parse(_).asInstanceOf[JObject])
    val v2_paper_rdd = sc.textFile(paper_temp_out_path_v2).map(parse(_).asInstanceOf[JObject])

    val v1_kv = v1_paper_rdd.map(j => (j.values("year").##.toString + j.values("title").##.toString, (j, 1)))
    val v2_kv = v2_paper_rdd.map(j => (j.values("year").##.toString + j.values("title").##.toString, (j, 2)))
    val final_json = (v1_kv union v2_kv)
        .reduceByKey((pair_1, pair_2) => {
          if (pair_1._2 == 1)
            (pair_1._1 merge pair_2._1, pair_2._2)
          else
            (pair_2._1 merge pair_1._1, 2)
        }).values.map( f => f._1 ).map(compact(_))
    final_json.saveAsTextFile(out_path)
    sparkSession.read.json(out_path).write.saveAsTable("oadv2.v1_v2_merged_table")

}
}
