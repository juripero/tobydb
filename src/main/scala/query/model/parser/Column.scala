package query.model.parser

case class Column(var name: String, var dType: EnumDataType.Value, var isNull: Boolean)

object Column {

  val PrimaryKey = "primary key"
  val NotNull = "not null"

  def createColumn(columnAsString: String) : Column = {
    var temp : String = null
    var isNull : Boolean = true
    if(columnAsString.toLowerCase.endsWith(PrimaryKey))
      temp = columnAsString.substring(0, columnAsString.length - PrimaryKey.length).trim
    else
      if(columnAsString.toLowerCase.endsWith(NotNull)) {
        temp = columnAsString.substring(0, columnAsString.length - NotNull.length).trim
        isNull = false
      }
    val div = columnAsString.split(" ")
    if(div.length > 2){
      println("Incorrect query format")
      return null
    }

    if(div.length > 1){
      val dt = getDataType(div(1))
      if(dt == null){
        println("Incorrect data type provided")
        return null
      }
      return new Column(div(0).trim, dt, isNull)
    }

    println("Incorrect query format")
    null
  }

  def getDataType(dataType: String) : EnumDataType.Value = {
    dataType match {
      case "tinyint" => EnumDataType.TinyInt
      case "smallint" => EnumDataType.SmallInt
      case "int" => EnumDataType.Int
      case "bigint" => EnumDataType.BigInt
      case "real" => EnumDataType.Real
      case "double" => EnumDataType.Double
      case "datetime" => EnumDataType.DateTime
      case "date" => EnumDataType.Date
      case "text" => EnumDataType.Text
      case _ => null
    }
  }
}
