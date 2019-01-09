package query.dml

import common.{DBHelper, Utility}
import datatype.base.DType
import io.IOHandler
import io.model.BaseCondition
import query.base.InputQuery
import query.model.parser.{Condition, Literal}
import query.model.result.Result

import scala.collection.mutable

case class Update(database: String, table: String, colName: String, literal: Literal, condition: Condition) extends InputQuery {

  override def executeQuery: Result = {
    val handler = new IOHandler
    val helper = new DBHelper(handler)
    val rCol = helper.fetchColumns(database, table)
    val colDT = helper.getDataTypesOfAllColumns(database, table)
    val internalCon = getSearchCondition(rCol, colDT)
    val updatedColInd = updatedListOfColIndexes(rCol)
    val updatedColVal = updatedListOfColValues(colDT)
    new Result(handler.updateRecord(database, table, internalCon, updatedColInd, updatedColVal, false))
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

    if(condition == null)
      return isColValid(rCol, false) && isDataTypeValid(colDT, rCol, false)
    else {
      if(!isColValid(rCol, true) || !isColValid(rCol, false) || isDataTypeValid(colDT, rCol, false)
      || isDataTypeValid(colDT, rCol, true)) return false
    }

    true
  }

  def isDataTypeValid(colDT: mutable.Map[String, Int], colList: mutable.MutableList[String], check: Boolean) : Boolean = {
    var invalidCol = ""
    val col = if(check) condition.column else colName
    val colVal = if(check) condition.literal else literal

    if(colList.contains(col)){
      val dtIndex = colDT(col)
      if(colVal.dataType != Utility.fetchModelDataType(dtIndex.toByte)){
        if(Utility.canUpdateLiteralDataType(colVal, dtIndex)) return true
        invalidCol = col
      }
    }

    if(invalidCol != ""){
      println(s"Value for $invalidCol is incorrect")
      return false
    }

    true
  }

  def isColValid(rCol: mutable.MutableList[String], check: Boolean) = {
    var isColValid = true
    var invalidCol = ""
    val tableCol = if(check) condition.column else colName
    if(!rCol.contains(tableCol.toLowerCase)){
      isColValid = false
      invalidCol = tableCol
    }

    if(!isColValid){
      println(s"$table.$colName entity not found")
      false
    }else
      true
  }

  def getSearchCondition(colList: mutable.MutableList[String], colDT: mutable.Map[String, Int]) =
    new BaseCondition(colList.indexOf(condition.column), Utility.numericOperator(condition.operator).toShort,
      DType.createSystemDType(condition.literal.value, colDT(condition.column).toByte))

  def updatedListOfColIndexes(rList: mutable.MutableList[String]) =
    mutable.MutableList[Byte](rList.indexOf(colName).toByte)

  def updatedListOfColValues(colDT: mutable.Map[String, Int]) =
    mutable.MutableList[Object](DType.createSystemDType(literal.value, colDT(colName).toByte))

}
