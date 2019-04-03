package pub.sha0w.ETL.graphProcess.csv

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import pub.sha0w.ETL.graphProcess.utils.HDFSUtils

import scala.collection.mutable.ListBuffer

object csvProcess {
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
    val otp = "/oad/out/csv"
    for (
    str <- Array("author_entity","author_paper_relation","fos_entity","fos_paper_relation","keyword_entity","keyword_paper_relation","paper_entity","reference_paper_relation","venue_entity","venue_paper_relationship")
    ) {
      process("oad." + str, otp + str, sparkSession)
      HDFSUtils.mergeFileByShell(otp + str, "/data/csv" + otp + ".csv")
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
  def process (tableName: String, outputPath : String, spark : SparkSession) : Unit ={
    val table = spark.read.table(tableName)
    table.write
      .format("csv")
      .option("header", "true")
      .option("delimiter","\t")
      .option("quote","\"")
      .save(outputPath)
  }
}
