package pub.sha0w.ETL.oadv2_combine.phase_two

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{ArrayType, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import utils.JedisImplSer

import scala.collection.mutable

object mainP_2 {
  val hive_v2_venue_table_name = "oadv2.venues"
  val hive_v2_author_table_name = "oadv2.authors"
  val hive_v2_paper_table_name = "oadv2.papers"

  val author_linking : String = "/oadv2/author_linking_pairs.txt"
  val venue_linking : String = "/oadv2/venue_linking_pairs.txt"
  val paper_linking : String = "/oadv2/paper_linking_pairs.txt"
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .set("spark.driver.maxResultSize", "4g")
      .set("spark.default.parallelism", "500")
      .setAppName("MY_APP_NAME")
    val sparkSession = SparkSession
      .builder()
      .enableHiveSupport()
      .config(sparkConf)
      .getOrCreate()
    val sc = sparkSession.sparkContext
    val sql = sparkSession.sqlContext
    val jedis : JedisImplSer = new JedisImplSer("10.0.88.50", 6379)
//    val jedis_broad : Broadcast[JedisImplSer] = sc.broadcast(jedis)
    //read table
    val paper_ds = sql.read.table(hive_v2_paper_table_name)
    val schema = paper_ds.schema
    val a_id_index = getAuthorIdIndex(schema)
    val a_index = getAuthorIndex(schema)
    val v_id_index = getVenueIdIndex(schema)
    val v_index = getVenueIndex(schema)
    val changed_rdd = paper_ds.rdd.map(r => {
      var ret = r
      val a_id = getAuthorId(r, a_index, a_id_index)
      if (a_id.isDefined) {
        val seq = a_id.get.map(s => {
          if (s != null)
            jedis.getJedis.get(s)
          else
            s
        })
        ret = _modifier(ret, a_index, a_id_index, seq)
      }
      val v_id = getVenueId(r, v_index, v_id_index)
      if (v_id.isDefined) {
        val str = jedis.getJedis.get(v_id.get)
        ret = modifier(ret, v_index, v_id_index, str)
      }
      ret
    })

    val sample = changed_rdd.take(10)
    sample.toList.foreach(println(_))
    println(paper_ds.show(10))
    val changed_ds = sql.createDataFrame(changed_rdd, schema)
    changed_ds.write.mode(SaveMode.Overwrite).json("/tmp/oadv2/paper_final")
  }
  def getAuthorIndex (root_schema : StructType) : Int = {
    root_schema.fieldIndex("authors")
  }
  def getAuthorIdIndex (root_schema : StructType) : Int = {
    root_schema(getAuthorIndex(root_schema))
      .dataType
      .asInstanceOf[ArrayType]
      .elementType
      .asInstanceOf[StructType]
      .fieldIndex("id")
  }

  def getVenueIndex (root_schema : StructType) : Int = {
    root_schema.fieldIndex("venue")
  }

  def getVenueIdIndex (root_schema : StructType) : Int = {
    root_schema(getVenueIndex(root_schema))
      .dataType
      .asInstanceOf[StructType]
      .fieldIndex("id")
  }

  def getVenueId (row : Row, index_1 : Int, index_2 : Int) : Option[String] = {
    val opt_1 = Option(row.getAs[GenericRowWithSchema](index_1))
    if (opt_1.isDefined)
      Option(opt_1.get.getAs[String](index_2))
    else Option(null)
  }

  def _getVenueId (row : Row, schema : StructType) : Option[String] = {
    val venue_index = getVenueIndex(schema)
    val venue_id_index = getVenueIdIndex(schema)
    getVenueId(row, venue_index, venue_id_index)
  }
  def getAuthorId (row : Row, index_1 : Int, index_2 : Int) : Option[Seq[String]] = {
    Option(
      row
        .getAs[mutable.WrappedArray[GenericRowWithSchema]](index_1)
        .map(gr => gr.getAs[String](index_2))
    )
  }
  def _getAuthorId (row : Row, schema : StructType) : Option[Seq[String]] = {
    val author_index = getAuthorIndex(schema)
    val author_id_index = getAuthorIdIndex(schema)
    getAuthorId(row, author_index, author_id_index)
  }
  def modifier(row: Row, index_1 : Int
              , index_2 : Int, other_string : String) : Row = {
    if (other_string == null) return row
    val seq : Seq[Any] = row.toSeq
    val changed_value: GenericRowWithSchema = {
      val inside = seq(index_1)
        .asInstanceOf[GenericRowWithSchema]
      val stuff = inside
        .toSeq
        .asInstanceOf[Seq[String]]
      new GenericRowWithSchema(stuff.updated(index_2, other_string).toArray, inside.schema)
    }
    println(changed_value)
    val changed: Seq[Any] = seq.updated(index_1, changed_value)
    println(changed)
    Row.fromSeq(changed)
  }

  def _modifier(row: Row, index_1 : Int
               , index_2 : Int, other_strings : Seq[String]) : Row = {
    val seq : Seq[Any] = row.toSeq
    val changed = seq.updated(index_1, {
      val inside = seq(index_1)
        .asInstanceOf[mutable.WrappedArray[GenericRowWithSchema]]
      var i = 0
      val row_seq = for (gr <- inside) yield {
        if(other_strings(i) == null) {
          i += 1
          gr
        }
        else {
          val stuff = gr.toSeq.asInstanceOf[Seq[String]]
          val other = stuff.updated(index_2, other_strings(i))
          i += 1
          new GenericRowWithSchema(other.toArray, gr.schema)
        }

      }
      row_seq
    })
    Row.fromSeq(changed)
  }
//  def field_modifier(row : Row, schema : Broadcast[StructType], jedis : JedisImplSer) : Row = {
//    val jedis_server = jedis.getJedis
//    val pre_vid = jedis_server.get(getVenueIdIndex(row, schema))
//  }
}
