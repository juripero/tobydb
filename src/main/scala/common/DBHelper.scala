package common

import datatype.{IntDT, TextDT}
import datatype.base.DType
import io.IOHandler
import io.model.BaseCondition
import query.QueryHandler

import scala.collection.mutable

class DBHelper(IOHandler: IOHandler) {

  def doesDBExist(database: String) = {
    if(database == null || database.length == 0){
      QueryHandler.incorrectCommand("", QueryHandler.UseHelpMessage)
      false
    }else
      IOHandler.checkIfDbExists(database)
  }

  def doesTableExist(database: String, table: String) = {
    if(database == null || table == null || database.length == 0 || table.length == 0){
      QueryHandler.incorrectCommand("", QueryHandler.UseHelpMessage)
      false
    }else
      IOHandler.checkIfTableExists(database, table)
  }

  def fetchColumns(database: String, table: String): mutable.MutableList[String] = {
    val conditions = mutable.MutableList(BaseCondition(CatalogHelper.ColumnsTableDatabaseName, BaseCondition.Equals, new TextDT(database)),
      BaseCondition(CatalogHelper.ColumnsTableTableName, BaseCondition.Equals, new TextDT(table)))

    val colNames : mutable.MutableList[String] = mutable.MutableList()
    val records = IOHandler.findRecord(Constants.CatalogDatabaseName, Constants.TobyColumns, conditions, false)

    records.foreach { rec =>
      val obj = rec.columnValues.get(CatalogHelper.ColumnsTableColumnName)
      colNames.+=(obj.asInstanceOf[DType].fetchString)
    }

    colNames
  }

  def checkNullConstraint(database: String, table: String, colMap: mutable.Map[String, Int]): Boolean ={
    val conditions = mutable.MutableList(BaseCondition(CatalogHelper.ColumnsTableDatabaseName, BaseCondition.Equals, new TextDT(database)),
      BaseCondition(CatalogHelper.ColumnsTableTableName, BaseCondition.Equals, new TextDT(table)))

    val records = IOHandler.findRecord(Constants.CatalogDatabaseName, Constants.TobyColumns, conditions, false)

    records.foreach { rec =>
      val nullValObj = rec.columnValues.get(CatalogHelper.ColumnsTableIsNullable)
      val obj = rec.columnValues.get(CatalogHelper.ColumnsTableColumnName)

      val isNull = nullValObj.asInstanceOf[DType].fetchString.toUpperCase.equals("YES")
      if(colMap.contains(obj.asInstanceOf[DType].fetchString) && !isNull){
        println("Error : Field " + obj.asInstanceOf[DType].fetchString + " cannot be null")
        return false
      }

    }

    true
  }

  def getDataTypesOfAllColumns(database: String, table: String): mutable.Map[String, Int] = {
    val conditions = mutable.MutableList(BaseCondition(CatalogHelper.ColumnsTableDatabaseName, BaseCondition.Equals, new TextDT(database)),
      BaseCondition(CatalogHelper.ColumnsTableTableName, BaseCondition.Equals, new TextDT(table)))

    val records = IOHandler.findRecord(Constants.CatalogDatabaseName, Constants.TobyColumns, conditions, false)
    val colDTMap = mutable.Map[String, Int]()

    records.foreach { rec =>
      val dtObj = rec.columnValues.get(CatalogHelper.ColumnsTableDataType)
      val obj = rec.columnValues.get(CatalogHelper.ColumnsTableColumnName)
      colDTMap(obj.asInstanceOf[DType].fetchString.toLowerCase) = Utility.stringToDataType(dtObj.asInstanceOf[DType].fetchString)
    }

    colDTMap
  }

  def getTablePrimaryKey(database: String, table: String) = {
    val conditions = mutable.MutableList(
      BaseCondition(CatalogHelper.ColumnsTableDatabaseName, BaseCondition.Equals, new TextDT(database)),
      BaseCondition(CatalogHelper.ColumnsTableTableName, BaseCondition.Equals, new TextDT(table)),
      BaseCondition(CatalogHelper.ColumnsTableColumnKey, BaseCondition.Equals, new TextDT(table)))

    val records = IOHandler.findRecord(Constants.CatalogDatabaseName, Constants.TobyColumns, conditions, true)
    if(records.nonEmpty)
      records.head.columnValues.get(CatalogHelper.ColumnsTableColumnName).asInstanceOf[DType].fetchString
    else
      ""
  }

  def getTableRecordCount(database: String, table: String) : Int = {
    val conditions = mutable.MutableList(
      BaseCondition(CatalogHelper.TablesTableDatabaseName, BaseCondition.Equals, new TextDT(database)),
      BaseCondition(CatalogHelper.TablesTableTableName, BaseCondition.Equals, new TextDT(table)))

    val records = IOHandler.findRecord(Constants.CatalogDatabaseName, Constants.TobyTables, conditions, true)
    if(records.nonEmpty)
      records.head.columnValues.get(CatalogHelper.TablesTableRecordCount).asInstanceOf[DType].fetchString.toInt
    else
      0
  }

  def checkIfPrimaryKeyValueExist(database: String, table: String, value: Int) : Boolean = {
    val condition = mutable.MutableList(BaseCondition(0, BaseCondition.Equals, new IntDT[Int](value, value == 0)))
    IOHandler.findRecord(database, table, condition, false).nonEmpty
  }

}

object DBHelper {

}