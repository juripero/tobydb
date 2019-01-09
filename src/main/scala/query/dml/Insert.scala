package query.dml

import common.{Constants, DBHelper, Utility}
import datatype._
import datatype.base.DType
import io.IOHandler
import io.model.RecordData
import query.base.InputQuery
import query.model.parser.Literal
import query.model.result.Result

import scala.util.control.Breaks._
import scala.collection.mutable

case class Insert(database: String, table: String, values: mutable.MutableList[Literal]
                  , columns: mutable.MutableList[String]) extends InputQuery {

  override def executeQuery: Result = {
    val handler = new IOHandler
    val helper = new DBHelper(handler)
    val rCol = helper.fetchColumns(database, table)
    val colDT = helper.getDataTypesOfAllColumns(database, table)
    val record = RecordData()
    generateRecords(record.columnValues, colDT, rCol)

    val rowId = getRowId(rCol)
    record.rowId = rowId
    record.populateSize

    val offset = handler.writeRecord(database, table, record)
    if(offset != -1) new Result(1)
    else {
      println("Unable to insert record")
      null
    }
  }

  override def validateQuery: Boolean = {
    val handler = new IOHandler
    if(!handler.checkIfTableExists(database, table)){
      println(s"$database.$table does not exist")
      return false
    }

    val helper = new DBHelper(handler)
    val rCol = helper.fetchColumns(database, table)
    val colDT = helper.getDataTypesOfAllColumns(database, table)

    if(columns != null){
      if(values.size > rCol.size){
        println("Mismatch between columns and values")
        return false
      }

      if(Utility.isDataTypeValid(colDT, rCol, values)) return false
    }else{
      if(columns.size > rCol.size){
        println("Mismatch between columns and values")
        return false
      }

      if(!isColValid(rCol) || !isColDTValid(colDT)) return false
    }

    if(!checkNullConstraint(rCol) || !checkPKConstraint(rCol)) return false
    true
  }

  def isColValid(rCol: mutable.MutableList[String]) : Boolean = {
    var isColValid = true
    var invalidCol = ""
    columns.foreach { c =>
      if(!rCol.contains(c.toLowerCase)){
        isColValid = false
        invalidCol = c
        break()
      }
    }

    if(!isColValid){
      println(s"Invalid Column : $invalidCol")
      false
    }else
      true
  }

  def checkNullConstraint(rCol: mutable.MutableList[String]) : Boolean = {
    val colList = mutable.Map[String, Int]()
    if(columns != null) (0 to columns.size).foreach(c => colList.put(columns(c), c))
    else (0 to values.size).foreach(e => colList.put(rCol(e), e))
    new DBHelper(new IOHandler).checkNullConstraint(database, table, colList)
  }

  def checkPKConstraint(rCol: mutable.MutableList[String]) : Boolean = {
    val helper = new DBHelper(new IOHandler)
    val pkCol = helper.getTablePrimaryKey(database, table)
    val colList = if(columns != null) columns else rCol

    if(pkCol.length > 0 && colList.contains(pkCol.toLowerCase)){
      val pkIndex = colList.indexOf(pkCol)
      if(helper.checkIfPrimaryKeyValueExist(database, table, values(pkIndex).value.toInt)){
        println("Duplicate entry found")
        return false
      }
    }

    true
  }

  def isColDTValid(colDT: mutable.Map[String, Int]) : Boolean = {
    var invalidCol = ""
    columns.foreach { c =>
      val dtIndex = colDT(c)
      val index = columns.indexOf(c)
      val literal = values(index)

      if(literal.dataType != Utility.fetchModelDataType(dtIndex.toByte)){
        if(!Utility.canUpdateLiteralDataType(literal, dtIndex)){
          invalidCol = c
          break()
        }
      }
    }

    if(!invalidCol.equals("")){
      println(s"Invalid value for column $invalidCol")
      return false
    }
    true
  }

  def generateRecords(colList: mutable.MutableList[Object], colMap: mutable.Map[String, Int], rCol: mutable.MutableList[String]) = {
    (0 to rCol.size).foreach { ind =>
      val col = rCol(ind)
      val dt = colMap(col).intValue()
      var ob : DType[_] = null
      if(columns != null){
        if(columns.contains(col)){
          val i = columns.indexOf(col)
          ob = getDataTypeObject(dt.toByte, values(i).value, false)
        }else {
          ob = getDataTypeObject(dt.toByte, null, true)
          ob.isNull = true
        }
      }else{
        if(ind < values.size){
          val i = rCol.indexOf(col)
          ob = getDataTypeObject(dt.toByte, values(i).value, false)
        }else {
          ob = getDataTypeObject(dt.toByte, null, true)
          ob.isNull = true
        }
      }
      colList.+=(ob)
    }
  }

  def getDataTypeObject(dType: Byte, value: String, isNull: Boolean) : DType[_] = {
    dType match {
      case Constants.TinyInt =>
        if(!isNull) new TinyIntDT[Byte](value.toByte, false) else new TinyIntDT[Byte]()
      case Constants.SmallInt =>
        if(!isNull) new SmallIntDT[Short](value.toShort, false) else new SmallIntDT[Short]()
      case Constants.Int =>
        if(!isNull) new IntDT[Int](value.toInt, false) else new IntDT[Int]()
      case Constants.BigInt =>
        if(!isNull) new BigIntDT[Long](value.toLong, false) else new BigIntDT[Long]()
      case Constants.Real =>
        if(!isNull) new RealDT[Float](value.toFloat, false) else new RealDT[Float]()
      case Constants.Double =>
        if(!isNull) new DoubleDT[Double](value.toDouble, false) else new DoubleDT[Double]()
      case Constants.Date =>
        if(!isNull) new DateDT[Long](value.toLong, false) else new DateDT[Long]()
      case Constants.DateTime =>
        if(!isNull) new DateTimeDT[Long](value.toLong, false) else new DateTimeDT[Long]()
      case _ =>
        if(!isNull) new TextDT(value, false) else new TextDT()
    }
  }

  def getRowId(rList: mutable.MutableList[String]) : Int = {
    val dbHelper = new DBHelper(new IOHandler)
    val rowCount = dbHelper.getTableRecordCount(database, table)
    val primaryCol = dbHelper.getTablePrimaryKey(database, table)
    if(primaryCol.length > 0){
      val index = if(primaryCol.length > 0) columns.indexOf(primaryCol) else rList.indexOf(primaryCol)
      Integer.parseInt(values(index).value)
    }else
      rowCount + 1
  }
}
