package pub.sha0w.ETL.util

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import pub.sha0w.ETL.graphProcess.obj.Entity

import scala.collection.mutable

object CSVUtils {
  // build relationship
  def mkEntityCSV (df : DataFrame, label : String, path : String) : Unit = {
    import sys.process._
    val t3 = s"hadoop fs -rmr $path"!
    val schema = df.schema
    val rdd = df.rdd
    val csvSchema = "ENTITY_ID:ID," + schema.filter(f => f.name != "id").map(f => {
      f.dataType match {
        case dt : StringType => f.name
        case dt : ArrayType => f.name + ":String[]"
        case _ => f.name
      }
    }).reduce((a,b) => a + "," + b) +  ",ENTITY_TYPE:LABEL"
    val final_rdd = rdd.map(row => {
      val e = new Entity()
      for (s <- schema) {
        if (s.name != "id") {
          e.addSchema(s.name)
        }
        if (!row.isNullAt(schema.fieldIndex(s.name))) {
          s.dataType match {
            case dt: ArrayType =>
              if (dt.elementType.isInstanceOf[StringType]) {
                e.addPro(s.name, row.getAs[mutable.WrappedArray[String]](s.name).toArray)
              } else {
                val elementType = dt.elementType.asInstanceOf[StructType]
                val elem: mutable.Seq[GenericRowWithSchema] = row.getAs[mutable.WrappedArray[GenericRowWithSchema]](s.name)
                val elem_arr: mutable.Seq[String] = elem.map(g => {
                  val elem_array = for (_s <- elementType) yield {
                    if (!g.isNullAt(g.schema.fieldIndex(_s.name))) {
                      _s.dataType match {
                        case dt: StringType => "(" + _s.name + "," + g.getAs[String](_s.name) + ")"
                        case dt: IntegerType => "(" + _s.name + "," + g.getAs[Int](_s.name).toString + ")"
                        case dt: LongType => "(" + _s.name + "," + g.getAs[Long](_s.name).toString + ")"
                        case _ => "(" + _s.name + "," + g.getAs[String](_s.name) + ")"
                    }
                  } else {
                      ""
                    }
                  }
                  val elem_arr = elem_array.filter(s => s != "")
                  if (elem_arr.nonEmpty) elem_arr.reduce((a, b) => a + ";" + b)
                  else null
                })
                e.addPro(s.name, elem_arr.toArray)
              }
            case df: LongType =>
              if (s.name == "id")
                e.addId(row.getAs[Long]("id").toString)
              else
                e.addPro(s.name, Array(row.getAs[Long](s.name).toString))
            case df: IntegerType =>
              e.addPro(s.name, Array(row.getAs[Int](s.name).toString))
            case df: StringType =>
              if (s.name == "id")
                e.addId(row.getAs[String]("id"))
              else
                e.addPro(s.name, Array(row.getAs[String](s.name)))
          }
        }
      }
      e.addLabel(label)
      e.toString
    })
    df.sparkSession.sparkContext.parallelize(Array(csvSchema)) ++ final_rdd saveAsTextFile path
  }
  // build entity csv
  def mkRelationCSV (df : DataFrame, role : String, `type` : String, path : String) : Unit = {
    import sys.process._
    val t3 = s"hadoop fs -rmr $path"!
    val finalRelationshipSchema = "ENTITY_ID:START_ID,role,ENTITY_ID:END_ID,RELATION_TYPE:TYPE"
    val finalRelationshipSchemaArray = df.sparkSession.sparkContext
      .parallelize(Array(finalRelationshipSchema))
    val schema = df.schema
    val rdd = df.rdd
    val final_rdd = rdd map (r => {
      (r.getAs[String](0), r.getAs[String](1))
    }) map (pair => {
      Array(pair._1 , role , pair._2 , `type`).reduce((a,a1) => a + "," + a1)
    })
    finalRelationshipSchemaArray ++ final_rdd saveAsTextFile path
  }
}

