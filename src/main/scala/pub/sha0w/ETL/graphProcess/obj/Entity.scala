package pub.sha0w.ETL.graphProcess.obj

import java.io.StringWriter

import org.apache.spark.sql.Row

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class Entity extends Serializable {
  var id : String = _
  var label : String = _
  var prop : mutable.HashMap[String, Array[String]] = mutable.HashMap[String, Array[String]]()
  var schema : mutable.ListBuffer[String] = new mutable.ListBuffer[String]()
  def this( id : String, label : String, prop : mutable.HashMap[String, Array[String]], schema : Seq[String]) {
    this()
    this.id = id
    this.label = label
    this.prop = prop
    this.schema = new mutable.ListBuffer[String]()
    this.schema.append(schema : _*)
  }
  def addPro (key : String, arr : Array[String]) : Unit = {
    prop.put(key, arr)
  }
  def addId(id : String) : Unit = {
    this.id = id
  }
  def addSchema (field : String) : Unit = {
    schema.append(field)
  }
  def addLabel (label : String) : Unit = {
    this.label = label
  }

  def propSeq : Array[String] = {
    var ret : ListBuffer[String] = new ListBuffer[String]()
    ret +:= id
    val l =  for (name <- schema) yield {
      val value : Array[String] = {
        if (prop.contains(name))
          prop(name)
        else
          Array("")
      }
      value
        .map(f => if (f == "") "\"\"" else f)
        .map(_.replaceAll(";", " "))
        .reduce((a,b) => a + ";" + b)
    }
    ret ++= l
    ret += label
    ret.map(s => {
      if (s.contains(","))
        "\"" + s + "\""
      else
        s
    }).toArray
  }

  override def toString: String = {
    this.propSeq.reduce((a,b) => a + "," + b)
  }
  def toRow : Row = {
    Row(propSeq)
  }
}
object Entity{
  def main(args: Array[String]): Unit = {
    val m : mutable.HashMap[String, Array[String]] = mutable.HashMap("sc1" ->Array("test1"), "sc2" -> Array(",2","1"))
    val e = new Entity("la,bel1", "i,d2", m, Array("sc1","sc2"))
    val stringWriter = new StringWriter()
  }
}