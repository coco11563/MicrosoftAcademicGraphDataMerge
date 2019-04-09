package pub.sha0w.ETL.graphProcess

import java.util.UUID

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import pub.sha0w.ETL.graphProcess.obj.{Author, Fos, Keyword, Venue}

import scala.collection.mutable

object Extract {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
      .set("spark.driver.maxResultSize", "6g")
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
    //    val select_paper_entity = paper_table.select("abstract","doc_type","doi","id","isbn","issn","issue","lang","n_citation","page_end","page_start","pdf","publisher","title","url","volume","year")
    //    select_paper_entity.write.mode(SaveMode.Overwrite).saveAsTable("oad.paper_entity")
    val venue = paper_table.select("id", "venue")
//    val jedis : JedisImplSer = new JedisImplSer("10.0.88.50", 6379)
    val venue_pair_schema = StructType(Array(StructField("paper_id", StringType, nullable = false),
      StructField("venue_id", StringType, nullable = false)))
    val venue_obj_rdd = venue.rdd.map(r => {
      (r.getAs[String]("id"), {
        val opt = Option(r.getAs[String]("venue"))
        if (opt.isEmpty) null
        else {
          val str = opt.get
          if (str.startsWith("{")) {
            val jobj = parse(str).asInstanceOf[JObject]
            val id = jobj.values.get("id")
            if (id.isEmpty) {
              val name = jobj.values.get("raw")
              if (name.isEmpty) null
              else new Venue(name.get.##.toString, name.get.asInstanceOf[String])
            } else {
              val name = jobj.values.get("raw")
              if (name.isEmpty) new Venue(id.get.asInstanceOf[String], null)
              else new Venue(id.get.asInstanceOf[String], name.get.asInstanceOf[String])
            }
          } else { // maybe just name
            val id = str.##.toString
            val name = str
            new Venue(id, name)
          }
        }
      })
    }).filter(pair => pair != null)
      .filter(pair => {pair._2 != null})
      .filter(pair => pair._2.id != null)
    val venue_key_pair_rdd = venue_obj_rdd.map(f => (f._1, f._2.id)).map(a => Row.fromTuple(a))
    val venue_key_pair_df = sparkSession.createDataFrame(venue_key_pair_rdd,
      venue_pair_schema )
    venue_key_pair_df.write.mode(SaveMode.Overwrite).saveAsTable("oad.venue_paper_relationship")
    val venue_table = sparkSession.read.table("oadv2.venues")
    val venue_rdd = venue_table.rdd.map(r => {
      Row.fromSeq(r.toSeq :+ "o")
    })
    var venue_schema = venue_table.schema
    venue_schema = new StructType((venue_schema.toList :+ StructField("source", StringType, nullable = false)).toArray)
    sparkSession.createDataFrame(venue_rdd
      , venue_schema).write.mode(SaveMode.Overwrite).saveAsTable("oad.venue_entity")
    val venue_append_rdd = venue_obj_rdd.values
      .map(v => v.parse(venue_schema))
      .map(r => (r.getAs[String](0), r))
      .reduceByKey((r1,r2) => r1)
      .values
    sparkSession.createDataFrame(venue_append_rdd, venue_schema)
      .write
      .mode(SaveMode.Append)
      .saveAsTable("oad.venue_entity")
    //author
    val author_table = sparkSession.read.table("oadv2.authors")
    val author_rdd = author_table.rdd.map(r => {
      Row.fromSeq(r.toSeq :+ "o")
    })
    var author_schema = author_table.schema
    author_schema =
      new StructType((author_schema.toList :+ StructField("source", StringType, nullable = false)).toArray)
    sparkSession.createDataFrame(author_rdd
      , author_schema).write.mode(SaveMode.Overwrite).saveAsTable("oad.author_entity")

    val author = paper_table.select("id","authors")
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
                au_id_aug
              } else { // org is null
                val au_id_aug = opt_name.get.##.toString
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

    val author_obj_rdd = author.rdd.map(r => {
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
                new Author(au_id_aug, name, org_name)
              } else { // org is null
                val au_id_aug = opt_name.get.##.toString
                val name = opt_name.get
                new Author(au_id_aug, name, null)
              }
            } else {
              null
            }
          } else {
            null
          }
        }).filter(p => p != null)}
    ).flatMap(a => a)

    val author_append_rdd = author_obj_rdd.map(v => v.parse(author_schema))
      .map(r => (r.getAs[String](0), r))
      .reduceByKey((r1,r2) => r1)
      .values
    sparkSession.createDataFrame(author_append_rdd, author_schema)
      .write
      .mode(SaveMode.Append)
      .saveAsTable("oad.author_entity")
    //fos
    val fos = paper_table.select("id", "fos") //ARRAY
    val fos_pair = fos.rdd.map(r => {
      (r.getAs[String]("id"), r.getAs[mutable.WrappedArray[String]]("fos"))
    }).filter(s => s._2 != null).map(pair => {
      pair._2.filter(s => s != null).map(s => (pair._1, s)).toList
    }).flatMap(a => a)

    val fos_row_rdd = fos_pair.map(f => {(f._1,f._2.##.toString)}).map(pair => Row.fromTuple(pair))
    sparkSession.createDataFrame(fos_row_rdd,
      new StructType(Array(StructField("id", StringType, nullable = false), StructField("fos_id", StringType, nullable = false))))
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable("oad.fos_paper_relation")
    //redis persisit
    val fos_obj_rdd = fos_pair
      .values
      .map(s => (s.##, s))
      .reduceByKey((a, b) => a)
      .values
      .map(str => {
        val name = str
        val id = str.##.toString
        new Fos(id, name)
      })

    val fos_schema = new StructType(
      Array(StructField("id", StringType, nullable = false),
        StructField("name", StringType, nullable = false)))
    val fos_append_rdd = fos_obj_rdd.map(
      v => v.parse(fos_schema))
      .map(r => (r.getAs[String](0), r))
      .reduceByKey((r1,r2) => r1)
      .values
    sparkSession.createDataFrame(fos_append_rdd, fos_schema)
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable("oad.fos_entity")
    //keywords
    val keywords = paper_table.select("id", "keywords") //ARRAY
    val keyword_pair = keywords.rdd.map(r => {
      (r.getAs[String]("id"), r.getAs[mutable.WrappedArray[String]]("keywords"))
    }).filter(s => s._2 != null).map(pair => {
      pair._2.filter(s => s != null).map(s => (pair._1, s)).toList
    }).flatMap(a => a)
    val keyword_rdd = keyword_pair.map(f => {(f._1,f._2.##.toString)}).map(pair => Row.fromTuple(pair))
    sparkSession.createDataFrame(keyword_rdd,
      new StructType(Array(StructField("id", StringType, nullable = false),
        StructField("keyword_id", StringType, nullable = false))))
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable("oad.keyword_paper_relation")
    //redis persisit
    val keyword_obj_rdd = keyword_pair
      .values
      .map(s => (s.##, s))
      .reduceByKey((a, b) => a)
      .values
      .map(str => {
        val id = str.##.toString
        val name = str
        new Keyword(id, name)
      })



    val keyword_schema = new StructType(
      Array(StructField("id", StringType, nullable = false),
        StructField("name", StringType, nullable = false)))
    val keyword_append_rdd = keyword_obj_rdd.map(
      v => v.parse(keyword_schema))
      .map(r => (r.getAs[String](0), r))
      .reduceByKey((r1,r2) => r1)
      .values
    sparkSession.createDataFrame(keyword_append_rdd, keyword_schema)
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable("oad.keyword_entity")

    // reference
    val reference = paper_table.select("id" , "references") //ARRAY
    val reference_pair = reference.rdd.map(r => {
      (r.getAs[String]("id"), r.getAs[mutable.WrappedArray[String]]("references"))
    }).filter(s => s._2 != null).map(pair => {
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
    author_entity.dropDuplicates("id").write.mode(SaveMode.Overwrite).saveAsTable("oad.author_entity_dup")


    val venue_entity = sparkSession.read.table("oad.venue_entity")
    venue_entity.dropDuplicates("id").write.mode(SaveMode.Overwrite).saveAsTable("oad.venue_entity_dup")


    //    val fos_entity = sparkSession.read.table("oad.fos_entity")
    //    fos_entity.dropDuplicates("id").write.mode(SaveMode.Overwrite).saveAsTable("oad.fos_entity")
    //
    //    val keyword_entity = sparkSession.read.table("oad.keyword_entity")
    //    keyword_entity.dropDuplicates("id").write.mode(SaveMode.Overwrite).saveAsTable("oad.keyord_entity")
  }
}
