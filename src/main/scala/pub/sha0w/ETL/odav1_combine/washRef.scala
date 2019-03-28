package pub.sha0w.ETL.odav1_combine

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import utils.JedisImplSer

import scala.collection.mutable

object washRef {
  val paper_temp_out_path =  "/tmp/oadv1/paper_combined_af"
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
    val sql = sparkSession.sqlContext
    val jedis : JedisImplSer = new JedisImplSer("10.0.88.50", 6379)
    val table_df = sparkSession.read.table( "oadv1.papers")
    val schema = table_df.schema
    val ref_index = schema.fieldIndex("references")
    val table_rdd = table_df.rdd
    val table_rdd_modified = table_rdd.map(r => {
      val row_seq = r.toSeq
      val ref = r.getAs[mutable.WrappedArray[String]](ref_index)
      if (ref == null || ref.isEmpty) r
      else {
        val ref_modified : mutable.WrappedArray[String] = ref.map(id => {
          val id_get = Option(jedis.getJedis.get(id))
          if (id_get.isDefined) id_get.get
          else id
        }).distinct
        Row.fromSeq(row_seq.updated(ref_index, ref_modified))
      }
    }
    )
    table_rdd_modified.take(10).foreach(println(_))
    val table_df_modified = sparkSession
      .createDataFrame(table_rdd_modified, schema)
    table_df_modified.write.mode(SaveMode.Overwrite).saveAsTable("oadv1.papers_after_wash")
    table_df_modified.write.mode(SaveMode.Overwrite).json(paper_temp_out_path)
  }
}
