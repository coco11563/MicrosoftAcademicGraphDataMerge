package pub.sha0w.ETL.oadv2_combine.phase_three

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import pub.sha0w.ETL.oadv2_combine.phase_two.mainP_2._
import utils.JedisImplSer

import scala.collection.mutable

object mainP_3 {
  // phase three paper.authors merged
  def combine(g1 : GenericRowWithSchema, g2 : GenericRowWithSchema): GenericRowWithSchema = {
    val seq1 = g1.toSeq
    val seq2 = g2.toSeq
    var i = 0
    val final_seq = for (cell <- seq1) yield {
      val ce = if (cell == null) {
        seq2(i)
      } else {
        cell
      }
      i += 1
      ce
    }
    new GenericRowWithSchema(final_seq.toArray, g1.schema)
  }

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
    val paper_ds = sql.read.json("/tmp/oadv2/paper_final")
    val schema = paper_ds.schema
    val a_id_index = getAuthorIdIndex(schema)
    val a_index = getAuthorIndex(schema)
    val paper_rdd = paper_ds.rdd.map(r => {
      var ret = r
      val author = getAuthor(r, a_index)
      val replace : Seq[GenericRowWithSchema] = if (author.isDefined) {
        val authors = author.get
        if (authors.nonEmpty) {
        val author_schema = authors.head.schema
        authors
          .map(r => (r.getAs[String](author_schema.fieldIndex("id")), r))
          .groupBy(r => r._1)
          .values
          .map(f => {
            f.reduce((pair_1,pair_2) => {
              (pair_1._1, combine(pair_1._2, pair_2._2))
            })
          }._2).toSeq
        } else authors
      } else {
        null
      }
      Row.fromSeq(ret.toSeq.updated(a_index, replace))
    })
    sparkSession.createDataFrame(paper_rdd, schema).write.mode(SaveMode.Overwrite).saveAsTable("oadv2.merged_table")
  }

  def getAuthor (row : Row, index_1 : Int) : Option[Seq[GenericRowWithSchema]] = {
    Option(
      row
        .getAs[mutable.WrappedArray[GenericRowWithSchema]](index_1)
    )
  }
}
