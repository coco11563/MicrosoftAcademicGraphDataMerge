package pub.sha0w.ETL.graphProcess

import java.util.UUID

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import utils.JedisImplSer

import scala.collection.mutable

object Extract {
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

    var paper_table = sparkSession.read.table("oadv2.v1_v2_merged_table")
    var schema = paper_table.schema
    val id_index = schema.fieldIndex("id")
    val year_index = schema.fieldIndex("year")
    val citation_index = schema.fieldIndex("n_citation")
    val paper_rdd = paper_table.rdd.map(r => {
      var seq = r.toSeq
      if (seq(id_index) == null) seq = seq.updated(id_index, UUID.randomUUID())
      seq = seq.updated(id_index, {
        val opt = Option(seq(id_index))
        if (opt.isDefined) opt.get
        else UUID.randomUUID()
      })
      seq = seq.updated(year_index, {
        val opt = Option(seq(year_index))
        if (opt.isDefined) opt.get.asInstanceOf[Int]
        else null
      })
      seq = seq.updated(citation_index, {
        val opt = Option(seq(citation_index))
        if (opt.isDefined) opt.get.asInstanceOf[Int]
        else null
      })
      Row.fromSeq(seq)
    })
    schema = StructType(schema
      .updated(year_index, StructField("year", IntegerType, nullable = true))
      .updated(citation_index, StructField("n_citation", IntegerType, nullable = true))
      .updated(id_index, StructField("id", StringType, nullable = false))
    )
    paper_table = sparkSession.createDataFrame(paper_rdd, schema)
    val select_paper_entity = paper_table.select("abstract","doc_type","doi","id","isbn","issn","issue","lang","n_citation","page_end","page_start","pdf","publisher","title","url","volume","year")
    select_paper_entity.write.mode(SaveMode.Overwrite).saveAsTable("oad.paper_entity")
    val venue = paper_table.select("id", "venue")
    val jedis : JedisImplSer = new JedisImplSer("10.0.88.50", 6379)
    val venue_key_pair_rdd = venue.rdd.map(r =>
    {
      (r.getAs[String]("id"), {
        val opt = Option(r.getAs[String]("venue"))
        if (opt.isEmpty) null
        else {
          val str = opt.get
          val jobj = parse(str).asInstanceOf[JObject]
          val value = Option(jobj.values("id")).getOrElse({
            val opt = Option(jobj.values("raw")) //need new venue
            if (opt.isEmpty) null //delete
            else {
              jedis.getJedis.hset("venue", opt.get.##.toString, s"{\"name\":\"${opt.get.asInstanceOf[String]}\"}")
              //TODO no venue id existed
              opt.get.##
            }
          }).asInstanceOf[String]
          value
        }
      })
    }).filter(pair => {pair._2 != null}).map(p => Row.fromTuple(p))
    val venue_key_pair_df = sparkSession.createDataFrame(venue_key_pair_rdd,
        StructType(Array(StructField("paper_id", StringType, nullable = false),
          StructField("venue_id", StringType, nullable = false)))
      )
    venue_key_pair_df.write.mode(SaveMode.Overwrite).saveAsTable("oad.venue_paper_relationship")
//    val venue_table = sparkSession.read.table("oadv2.venues")
//    val venue_shcema = venue_table.schema
//
    val author = paper_table.select("id","authors")
    val author_index = schema.fieldIndex("authors")
    val au_inside_schema = schema.fields(author_index).asInstanceOf[ArrayType]
    val author_kv_rdd = author.rdd.map(r => (r.getAs[String]("id"), {
      val au = r.getAs[mutable.WrappedArray[GenericRowWithSchema]]("authors")
      au.filter(f => f != null)
        .map(rws => {
          if (rws.getAs[String]("id") == null) {
            val opt_name = Option(rws.getAs[String]("name"))
            val opt_org = Option(rws.getAs[String]("org"))
            if (opt_name.isDefined) { // TODO no id existed
              if (opt_org.isDefined) {
                val au_id_aug = opt_name.get.##.toString + opt_org.get.##.toString
                //save as json
                jedis.getJedis.hset("authors", au_id_aug, s"{\"org\":\"${opt_org.get}\",\"name\":\"${opt_name.get}\"}")
                au_id_aug
              } else {
                val au_id_aug = opt_name.get.##.toString
                jedis.getJedis.hset("authors", au_id_aug, s"{\"name\":\"${opt_name.get}\"}")
                au_id_aug
              }
            } else {
              null
            }
          } else {
            rws.getAs[String]("id")
          }
        }).filter(p => p != null)})
    ).map(r => {
      r._2.map(f => (r._1, f)).toList
    }).flatMap[(String, String)](a => a) //problem
      .map(t => {Row.fromTuple(t)})

    sparkSession
      .createDataFrame(author_kv_rdd, new StructType(Array(StructField("paper_id", StringType, nullable = false),
        StructField("author_id", StringType, nullable = false))
      )).write.mode(SaveMode.Overwrite).saveAsTable("oad.author_paper_relation")


    val fos = paper_table.select("id", "fos") //ARRAY
    val fos_pair = fos.rdd.map(r => {
      (r.getAs[String]("id"), r.getAs[mutable.WrappedArray[String]]("fos"))
    }).map(pair => {
      pair._2.filter(s => s != null).map(s => (pair._1, s)).toList
    }).flatMap(a => a)

    //redis persisit
    fos_pair
      .values
      .map(s => (s.##, s))
      .reduceByKey((a, b) => a)
      .values
      .foreach(str => {
        jedis
          .getJedis
          .hset("fos", str.##.toString, s"{\"name\":\"$str\"}")
      })
    val fos_row_rdd = fos_pair.map(f => {(f._1,f._2.##.toString)}).map(pair => Row.fromTuple(pair))
    sparkSession.createDataFrame(fos_row_rdd,
      new StructType(Array(StructField("id", StringType, nullable = false), StructField("fos_id", StringType, nullable = false))))
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable("oad.fos_paper_relation")

    val keywords = paper_table.select("id", "keywords") //ARRAY
    val keyword_pair = keywords.rdd.map(r => {
      (r.getAs[String]("id"), r.getAs[mutable.WrappedArray[String]]("keywords"))
    }).map(pair => {
      pair._2.filter(s => s != null).map(s => (pair._1, s)).toList
    }).flatMap(a => a)

    //redis persisit
    keyword_pair
      .values
      .map(s => (s.##, s))
      .reduceByKey((a, b) => a)
      .values
      .foreach(str => {
        jedis
          .getJedis
          .hset("keywords", str.##.toString, s"{\"name\":\"$str\"}")
      })
    val keyword_rdd = keyword_pair.map(f => {(f._1,f._2.##.toString)}).map(pair => Row.fromTuple(pair))
    sparkSession.createDataFrame(keyword_rdd,
      new StructType(Array(StructField("id", StringType, nullable = false),
        StructField("keyword_id", StringType, nullable = false))))
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable("oad.keyword_paper_relation")




    val reference = paper_table.select("id" , "references") //ARRAY

    val reference_pair = reference.rdd.map(r => {
      (r.getAs[String]("id"), r.getAs[mutable.WrappedArray[String]]("references"))
    }).map(pair => {
      pair._2.filter(s => s != null).map(s => (pair._1, s)).toList
    }).flatMap(a => a)

    //redis persisit
    reference_pair
      .values
      .map(s => (s.##, s))
      .reduceByKey((a, b) => a)
      .values
      .foreach(str => {
        jedis
          .getJedis
          .hset("reference", str.##.toString, s"{\"name\":\"$str\"}")
      })
    val reference_rdd = reference_pair.map(f => {(f._1,f._2.##.toString)}).map(pair => Row.fromTuple(pair))
    sparkSession.createDataFrame(reference_rdd,
      new StructType(Array(StructField("id", StringType, nullable = false),
        StructField("reference_id", StringType, nullable = false))))
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable("oad.reference_paper_relation")
  }
}
