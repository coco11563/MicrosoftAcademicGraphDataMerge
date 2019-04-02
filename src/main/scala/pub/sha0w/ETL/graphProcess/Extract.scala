package pub.sha0w.ETL.graphProcess

import java.util.UUID

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import pub.sha0w.ETL.graphProcess.obj.{Author, Fos, Keyword, Venue}
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
    val venue_accumulator = sparkSession.sparkContext.collectionAccumulator[Venue]
    val author_accumulator = sparkSession.sparkContext.collectionAccumulator[Author]
    val fos_accumulator = sparkSession.sparkContext.collectionAccumulator[Fos]
    val keyword_accumulator = sparkSession.sparkContext.collectionAccumulator[Keyword]
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
        if (opt.isDefined) opt.get.asInstanceOf[Double].toInt
        else null
      })
      seq = seq.updated(citation_index, {
        val opt = Option(seq(citation_index))
        if (opt.isDefined) opt.get.asInstanceOf[Double].toInt
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
          if (str.startsWith("{")) {
            val jobj = parse(str).asInstanceOf[JObject]
            val value = jobj.values.getOrElse("id",{
              val opt = jobj.values.get("raw") //need new venue
              if (opt.isEmpty) null //delete
              else {
                val id = opt.get.##.toString
                val name =  opt.get.asInstanceOf[String]
                venue_accumulator.add(new Venue(id, name))
                id
              }
            }.asInstanceOf[String])
            value
          } else { // maybe just name
            val id = str.##.toString
            val name = str
            venue_accumulator.add(new Venue(id, name))
            id
          }
        }
      })
    }).filter(pair => {pair._2 != null}).map(p => Row.fromTuple(p))
    val venue_key_pair_df = sparkSession.createDataFrame(venue_key_pair_rdd,
        StructType(Array(StructField("paper_id", StringType, nullable = false),
          StructField("venue_id", StringType, nullable = false)))
      )
    venue_key_pair_df.write.mode(SaveMode.Overwrite).saveAsTable("oad.venue_paper_relationship")
    val venue_table = sparkSession.read.table("oadv2.venues")
    val venue_rdd = venue_table.rdd.map(r => {
      Row.fromSeq(r.toSeq ++ "o")
    })
    var venue_schema = venue_table.schema
    venue_schema = new StructType((venue_schema.toList :+ StructField("source", StringType, nullable = false)).toArray)
    sparkSession.createDataFrame(venue_rdd
    , venue_schema).write.mode(SaveMode.Overwrite).saveAsTable("oad.venue_entity")

    import scala.collection.JavaConversions._
    val venue_append_rdd = sparkSession
      .sparkContext
      .parallelize(venue_accumulator.value.map(v => v.parse(venue_schema)))
      .map(r => (r.getAs[String](0), r))
      .reduceByKey((r1,r2) => r1)
      .values
    sparkSession.createDataFrame(venue_append_rdd, venue_schema)
      .write
      .mode(SaveMode.Append)
      .saveAsTable("oad.venue_entity")

    venue_accumulator.reset()
    val author = paper_table.select("id","authors")
    val author_index = schema.fieldIndex("authors")
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
                val org_name = opt_org.get
                val name = opt_name.get
                author_accumulator.add(new Author(au_id_aug, name, org_name))
                au_id_aug
              } else { // org is null
                val au_id_aug = opt_name.get.##.toString
                val name = opt_name.get
                author_accumulator.add(new Author(au_id_aug, name, null))
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

    val author_table = sparkSession.read.table("oadv2.authors")
    val author_rdd = author_table.rdd.map(r => {
      Row.fromSeq(r.toSeq ++ "o")
    })
    var author_schema = author_table.schema
    author_schema =
      new StructType((author_schema.toList :+ StructField("source", StringType, nullable = false)).toArray)
    sparkSession.createDataFrame(author_rdd
      , author_schema).write.mode(SaveMode.Overwrite).saveAsTable("oad.author_entity")
    val author_append_rdd = sparkSession
      .sparkContext
      .parallelize(author_accumulator.value.map(v => v.parse(author_schema)))
      .map(r => (r.getAs[String](0), r))
      .reduceByKey((r1,r2) => r1)
      .values
    sparkSession.createDataFrame(author_append_rdd, author_schema)
      .write
      .mode(SaveMode.Append)
      .saveAsTable("oad.author_entity")

    author_accumulator.reset()


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
        val name = str
        val id = str.##.toString
        fos_accumulator.add(new Fos(id, name))
      })
    val fos_row_rdd = fos_pair.map(f => {(f._1,f._2.##.toString)}).map(pair => Row.fromTuple(pair))
    sparkSession.createDataFrame(fos_row_rdd,
      new StructType(Array(StructField("id", StringType, nullable = false), StructField("fos_id", StringType, nullable = false))))
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable("oad.fos_paper_relation")
    val fos_schema = new StructType(
      Array(StructField("id", StringType, false),
        StructField("name", StringType, false)))
    val fos_append_rdd = sparkSession
      .sparkContext
      .parallelize(fos_accumulator.value.map(
        v => v.parse(fos_schema)))
      .map(r => (r.getAs[String](0), r))
      .reduceByKey((r1,r2) => r1)
      .values
    sparkSession.createDataFrame(fos_append_rdd, fos_schema)
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable("oad.fos_entity")

    fos_accumulator.reset()

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
        val id = str.##.toString
        val name = str
        keyword_accumulator.add(new Keyword(id, name))
      })
    val keyword_rdd = keyword_pair.map(f => {(f._1,f._2.##.toString)}).map(pair => Row.fromTuple(pair))
    sparkSession.createDataFrame(keyword_rdd,
      new StructType(Array(StructField("id", StringType, nullable = false),
        StructField("keyword_id", StringType, nullable = false))))
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable("oad.keyword_paper_relation")


    val keyword_schema = new StructType(
      Array(StructField("id", StringType, false),
        StructField("name", StringType, false)))
    val keyword_append_rdd = sparkSession
      .sparkContext
      .parallelize(keyword_accumulator.value.map(
        v => v.parse(keyword_schema)))
        .map(r => (r.getAs[String](0), r))
        .reduceByKey((r1,r2) => r1)
        .values
    sparkSession.createDataFrame(keyword_append_rdd, keyword_schema)
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable("oad.keyword_entity")
    fos_accumulator.reset()


    val reference = paper_table.select("id" , "references") //ARRAY
    val reference_pair = reference.rdd.map(r => {
      (r.getAs[String]("id"), r.getAs[mutable.WrappedArray[String]]("references"))
    }).map(pair => {
      pair._2.filter(s => s != null).map(s => (pair._1, s)).toList
    }).flatMap(a => a)
    val reference_rdd = reference_pair.map(pair => Row.fromTuple(pair))
    sparkSession.createDataFrame(reference_rdd,
      new StructType(Array(StructField("id", StringType, nullable = false),
        StructField("reference_id", StringType, nullable = false))))
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable("oad.reference_paper_relation")


    //distinct
//    val paper_entity = sparkSession.read.table("oad.paper_entity")
//    paper_entity.dropDuplicates("id").write.mode(SaveMode.Overwrite).saveAsTable("oad.paper_entity")

    val author_entity = sparkSession.read.table("oad.author_entity")
    author_entity.dropDuplicates("id").write.mode(SaveMode.Overwrite).saveAsTable("oad.author_entity")


    val venue_entity = sparkSession.read.table("oad.venue_entity")
    venue_entity.dropDuplicates("id").write.mode(SaveMode.Overwrite).saveAsTable("oad.venue_entity")


    //    val fos_entity = sparkSession.read.table("oad.fos_entity")
//    fos_entity.dropDuplicates("id").write.mode(SaveMode.Overwrite).saveAsTable("oad.fos_entity")
//
//    val keyword_entity = sparkSession.read.table("oad.keyword_entity")
//    keyword_entity.dropDuplicates("id").write.mode(SaveMode.Overwrite).saveAsTable("oad.keyord_entity")
  }
}
