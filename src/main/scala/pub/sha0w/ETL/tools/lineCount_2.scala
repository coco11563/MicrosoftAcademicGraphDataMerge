package pub.sha0w.ETL.tools

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.json4s._
import org.json4s.jackson.JsonMethods._


object lineCount_2 {
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

    val tables = sc.textFile("/oadv2/mag/authors/*")
    tables.map(str => {
      val json: Map[String, Any] = parse(str).asInstanceOf[JObject].values
      (json.getOrElse("id", null), str.length - {
        val pubs_len = getLen("pubs", json)
        val org_len = getLen("orgs", json)
        val tags_len = getLen("tags", json)
        pubs_len + org_len + tags_len
      })
    }).sortBy(a => a._2)
      .saveAsTextFile("/tmp/authors.len.check")
  }


  def getLen(name:String, json : Map[String, Any]) : Int = {
    val pubs = json.get(name)
    if (pubs.isEmpty) 0 else {
      compact(pubs.get.asInstanceOf[JArray]).length
    }
  }
}
