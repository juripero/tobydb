package query.dml

import common.{DBHelper, Utility}
import datatype.base.DType
import io.IOHandler
import io.model.BaseCondition
import query.base.InputQuery
import query.model.parser.Condition
import query.model.result.Result

import scala.collection.mutable

case class Delete(database: String, table: String, conditions: mutable.MutableList[Condition], isInternal: Boolean)
  extends InputQuery {

  override def executeQuery: Result = {
    var rowCount = 0
    val handler = new IOHandler
    if(conditions == null) rowCount = handler.deleteRecord(database, table, mutable.MutableList[BaseCondition]())
    else {
      val conditionList = mutable.MutableList[BaseCondition]()
      conditions.foreach { c =>
        val rCol = new DBHelper(handler).fetchColumns(database, table)
        val idx = rCol.indexOf(c.column)
        val dType = DType.createDType(c.literal)
        val internalCond = new BaseCondition(idx.toByte, Utility.numericOperator(c.operator).toShort, dType.value)
        conditionList.+=(internalCond)
      }
    }

    new Result(rowCount, isInternal)
  }

  override def validateQuery: Boolean = {
    val handler = new IOHandler
    if(!handler.checkIfTableExists(database, table)){
      println(s"$database.$table not found")
      return false
    }

    if(conditions != null){
      val rCol = new DBHelper(handler).fetchColumns(database, table)
      val colDT = new DBHelper(handler).getDataTypesOfAllColumns(database, table)
      conditions.foreach { c =>
        if(!checkIfColAndCondAreValid(rCol) || Utility.isDataTypeOfConditionValid(colDT, rCol, c)) return false
      }
    }

    true
  }

  def checkIfColAndCondAreValid(rCol: mutable.MutableList[String]) : Boolean = {

    var valid = true
    var invalidCol: String = ""

    conditions.foreach { c =>
      if(!rCol.contains(c.column.toLowerCase)){
        invalidCol = c.column
        valid = false
      }

      if(!valid){
        println(s"$invalidCol is not present in $table")
        return false
      }
    }

    true
  }

}
