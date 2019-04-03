package pub.sha0w.ETL.graphProcess.obj

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
//import java.io.{ObjectInputStream, ObjectOutputStream, ObjectStreamException, Serializable}
trait SerializableObject extends Serializable {
  def parse(schema : StructType) : Row

//  def writeObject(out: ObjectOutputStream) : Unit
//  def readObject(in: ObjectInputStream) : Unit
//  def readObjectNoData(): Unit
}

class Venue( val id : String, val name : String) extends SerializableObject {
  def parse(schema: StructType): Row = {
    Row.fromSeq({
      for (field <- schema) yield {
        if (field.name == "id") id
        else if (field.name == "displayname") name
        else if (field.name == "source") "g"
        else null
      }
    })
  }

//  override def writeObject(out: ObjectOutputStream): Unit = ???
//
//  override def readObject(in: ObjectInputStream): Unit = ???
//
//  override def readObjectNoData(): Unit = ???
//  override def writeObject(out: ObjectOutputStream): Unit = {
//    out.defaultWriteObject()
//  }
//
//  override def readObject(in: ObjectInputStream): Unit = {
//    in.defaultReadObject()
//  }
//
//  override def readObjectNoData(): Unit = {}
}

class Author(val id : String, val name : String, val org : String) extends SerializableObject {
  override def parse(schema: StructType): Row = {
    Row.fromSeq({
      for (field <- schema) yield {
        if (field.name == "id") id
        else if (field.name == "name") name
        else if (field.name == "org") org
        else if (field.name == "orgs") {
          if (org == null) Array()
          else Array(org)
        }
        else if (field.name == "source") "g"
        else null
      }
    })
  }

//  override def writeObject(out: ObjectOutputStream): Unit = ???
//
//  override def readObject(in: ObjectInputStream): Unit = ???
//
//  override def readObjectNoData(): Unit = ???
}
class Fos(val id : String, val name : String) extends SerializableObject {
  override def parse(schema: StructType): Row = {
    Row.fromSeq({
      for (field <- schema) yield {
        if (field.name == "id") id
        else if (field.name == "name") name
        else if (field.name == "source") "g"
        else null
      }
    })
  }
//
//  override def writeObject(out: ObjectOutputStream): Unit = ???
//
//  override def readObject(in: ObjectInputStream): Unit = ???
//
//  override def readObjectNoData(): Unit = ???
}
class Keyword (val id : String, val name : String) extends SerializableObject {
  override def parse(schema: StructType): Row = {
    Row.fromSeq({
      for (field <- schema) yield {
        if (field.name == "id") id
        else if (field.name == "name") name
        else if (field.name == "source") "g"
        else null
      }
    })
  }
//
//  override def writeObject(out: ObjectOutputStream): Unit = ???
//
//  override def readObject(in: ObjectInputStream): Unit = ???
//
//  override def readObjectNoData(): Unit = ???
}

