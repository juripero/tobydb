package common

import java.io.File
import java.text.SimpleDateFormat
import java.time.{Instant, ZoneId, ZonedDateTime}
import java.util.Date

import datatype._
import datatype.base.DTypeNum
import query.model.parser.{Condition, EnumDataType, Literal, Operator}

import scala.collection.mutable

object Utility {

  def getDatabasePath(database: String) = Constants.DataDirectoryName + "/" + database

  def validateClass(obj: Object): Byte = {

    if (obj.isInstanceOf[TinyIntDT]) return Constants.TinyInt
    if (obj.isInstanceOf[SmallIntDT]) return Constants.SmallInt
    if (obj.isInstanceOf[IntDT]) return Constants.Int
    if (obj.isInstanceOf[BigIntDT]) return Constants.BigInt
    if (obj.isInstanceOf[RealDT]) return Constants.Real
    if (obj.isInstanceOf[DoubleDT]) return Constants.Double
    if (obj.isInstanceOf[DateDT]) return Constants.Date
    if (obj.isInstanceOf[DateTimeDT]) return Constants.DateTime
    if (obj.isInstanceOf[TextDT]) return Constants.Text
    Constants.InvalidClass
  }

  def stringToDataType(dataType: String): Byte = {
    dataType match {
      case "TINYINT" => Constants.TinyInt
      case "SMALLINT" => Constants.SmallInt
      case "INT" => Constants.Int
      case "BIGINT" => Constants.BigInt
      case "REAL" => Constants.Real
      case "DOUBLE" => Constants.Double
      case "DATE" => Constants.Date
      case "DATETIME" => Constants.DateTime
      case "TEXT" => Constants.Text
      case _ => Constants.InvalidClass
    }
  }

  def fetchModelDataType(dataType: Byte) = {
    dataType match {
      case Constants.TinyInt => EnumDataType.TinyInt
      case Constants.SmallInt => EnumDataType.SmallInt
      case Constants.Int => EnumDataType.Int
      case Constants.BigInt => EnumDataType.BigInt
      case Constants.Real => EnumDataType.Real
      case Constants.Double => EnumDataType.Double
      case Constants.Date => EnumDataType.Date
      case Constants.DateTime => EnumDataType.DateTime
      case Constants.Text => EnumDataType.Text
      case _ => null
    }
  }

  def validateDateFormat(date: String): Boolean = {
    val format = new SimpleDateFormat("yyyy-MM-dd")
    format.setLenient(true)

    try {
      format.parse(date)
    } catch {
      case _: Exception => return false
    }
    true
  }

  def validateDateTimeFormat(date: String): Boolean = {
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    format.setLenient(true)

    try {
      format.parse(date)
    } catch {
      case _: Exception => return false
    }
    true
  }

  def numericOperator(operator: Operator.Value) : Int = {
    operator match {
      case Operator.Equals => DTypeNum.Equals
      case Operator.GreaterThanEqual => DTypeNum.GreaterThanEquals
      case Operator.GreaterThan => DTypeNum.GreaterThan
      case Operator.LessThanEqual => DTypeNum.LessThanEquals
      case Operator.LessThan => DTypeNum.LessThan
      case _ => null
    }
  }

  def isDataTypeOfConditionValid(colDt: mutable.Map[String, Int], columns: mutable.MutableList[String], condition: Condition): Boolean = {
    columns.contains(condition.fetchColumn) match {
      case true =>
        val index: Int = colDt.getOrElse(condition.fetchColumn, 0)
        val literal = condition.fetchLiteral

        if (literal.dataType != Utility.fetchModelDataType(index.toByte)) {
          if (Utility.canUpdateLiteralDataType(literal, index))
            return true
        }

        false

      case _ => false
    }
  }

  def getEpochDate(value: String, isDate: Boolean): Long = {
    val formatter = isDate match {
      case true => new SimpleDateFormat("yyyy-MM-dd")
      case false => new SimpleDateFormat("yyyy-MM-dd HH:mm:sss")
    }

    formatter.setLenient(true)
    try {
      val date = formatter.parse(value)
      val zdt = ZonedDateTime.ofInstant(date.toInstant, ZoneId.systemDefault())
      zdt.toInstant.toEpochMilli / 1000
    } catch {
      case _: Exception => 0
    }
  }

  def getEpochDateAsString(value: Long, isDate: Boolean) = {
    val zoneId = ZoneId.of("America/Chicago")
    val instant = Instant.ofEpochSecond(value)
    val zdt = ZonedDateTime.ofInstant(instant, zoneId)
    val date = Date.from(zdt.toInstant)

    val formatter = isDate match {
      case true => new SimpleDateFormat("yyyy-MM-dd")
      case false => new SimpleDateFormat("yyyy-MM-dd HH:mm:sss")
    }
    formatter.setLenient(true)
    formatter.format(date)
  }

  def isDataTypeValid(colDT: mutable.Map[String, Int], columns: mutable.MutableList[String]
                      , values: mutable.MutableList[Literal]) : Boolean = {
    var invalidLiteral : Literal = null
    var invalidCol : String = null
    (0 to columns.size).foreach { in =>
      val col = columns(in)
      val dType = colDT(col)
      val literal = values(in)

      if(literal.dataType != fetchModelDataType(dType.toByte)){
        if( !canUpdateLiteralDataType(literal, dType))
          invalidLiteral = literal
          invalidCol = col
      }
    }

    if(invalidCol == null)
      true
    else {
      println(s"Invalid Value: ${invalidLiteral.value} for column: $invalidCol")
      false
    }
  }

  def recursiveDelete(file: File) : Boolean = {
    if(file == null)
      return true

    if(file.isDirectory){
      file.listFiles().foreach { f =>
        if(f.isFile)
          return f.delete()
        else
          return recursiveDelete(f)
      }
    }

    file.delete()
  }

  def canUpdateLiteralDataType(literal: Literal, columnType: Int): Boolean = {

    columnType match {
      case Constants.TinyInt =>
        if(literal.dataType == EnumDataType.Int && literal.value.toInt <= Byte.MaxValue ){
          literal.dataType = EnumDataType.TinyInt
          true
        } else false

      case Constants.SmallInt =>
        if(literal.dataType == EnumDataType.Int && literal.value.toInt <= Short.MaxValue ){
          literal.dataType = EnumDataType.SmallInt
          true
        } else false

      case Constants.BigInt =>
        if(literal.dataType == EnumDataType.Int && literal.value.toInt <= Long.MaxValue ){
          literal.dataType = EnumDataType.BigInt
          true
        } else false

      case Constants.Double =>
        if(literal.dataType == EnumDataType.Real){
          literal.dataType = EnumDataType.Double
          true
        } else false

      case _ => false
    }

  }
}
