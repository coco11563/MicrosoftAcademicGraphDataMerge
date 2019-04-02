package pub.sha0w.ETL.graphProcess.obj

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

trait SerializableObject extends Serializable {
  def parse(schema : StructType) : Row
}

class Venue(id : String, name : String) extends SerializableObject {
  override def parse(schema: StructType): Row = {
    Row.fromSeq({
      for (field <- schema) yield {
        if (field.name == "id") id
        else if (field.name == "displayname") name
        else if (field.name == "source") "g"
        else null
      }
    })
  }
}

class Author(id : String, name : String, org : String) extends SerializableObject {
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
}
class Fos(id : String, name : String) extends SerializableObject {
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
}
class Keyword (id : String, name : String) extends SerializableObject {
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
}

