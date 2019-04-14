package pub.sha0w.ETL.graphProcess.csv

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import pub.sha0w.ETL.util.CSVUtils

import scala.collection.mutable.ListBuffer

object csvProcess {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
      .set("spark.driver.maxResultSize", "6g")
      .setAppName("MY_APP_NAME")

    val sparkSession: SparkSession = SparkSession
      .builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()
    val sc = sparkSession.sparkContext
    sc.setCheckpointDir("/tmp/oadv2/checkpoint_location")
    val otp = "/oad/out/csv"
    for (
    str <- Array(
//      "venue_entity",
//      "venue_paper_relationship",
//      "author_entity","author_paper_relation",
//      "fos_entity","fos_paper_relation","keyword_entity",
//      "keyword_paper_relation",
      "paper_entity",
//      "reference_paper_relation"
      "author_entity"
    )
    ) {
      val table = sparkSession.read.table("oad." + str)
      str match {
        case "author_entity" => CSVUtils.mkEntityCSV(table, "AUTHOR", otp + "/" + str )
        case "author_paper_relation" => CSVUtils.mkRelationCSV(table, "write", "write", otp+ "/" + str)
        case "fos_entity" => CSVUtils.mkEntityCSV(table, "FOS", otp+ "/" + str)
        case "fos_paper_relation" => CSVUtils.mkRelationCSV(table, "fos", "fos", otp+ "/" + str)
        case "keyword_entity" => CSVUtils.mkEntityCSV(table, "KEYWORD", otp+ "/" + str)
        case "keyword_paper_relation" => CSVUtils.mkRelationCSV(table, "keyword", "keyword", otp+ "/" + str)
        case "reference_paper_relation" => CSVUtils.mkRelationCSV(table, "cite", "cite", otp+ "/" + str)
        case "venue_entity" => CSVUtils.mkEntityCSV(table, "VENUE", otp+ "/" + str)
        case "paper_entity" => CSVUtils.mkEntityCSV(table, "PAPER", otp + "/" + str)
        case "venue_paper_relationship" => CSVUtils.mkRelationCSV(table, "pub_on", "venue", otp+ "/" + str)
        case _ => throw new Exception("WRONG NAME")
      }
//      process(, otp + str, sparkSession)
//      HDFSUtils.mergeFileByShell(otp + "/" + str, "/data/csv" +  "/" + str + ".csv")
    }
  }
  def washForCsv(df : DataFrame) : DataFrame = {
    val schema = df.schema
    var rdd = df.rdd
    for (field <- schema) {
      rdd = if (field.dataType == StringType) {
        val index = schema.fieldIndex(field.name)
        rdd.map(r => {
          var changeValue = r.getString(index)
          if (changeValue != null) {
            changeValue = changeValue
              .replaceAll("\r", " ")
              .replaceAll("\n", " ")
          }
          rowChangeValue(r, index, changeValue)
        })
      } else rdd
    }
    df.sparkSession.createDataFrame(rdd, schema)
  }

  def rowChangeValue(row : Row, index : Int, value : String) : Row ={
    val li = ListBuffer[Any]()
    var i = 0
    for (v <- row.toSeq.toList) {
      if (i == index) li += value
      else li += v
      i += 1
    }
    Row.fromSeq(li)
  }
}
