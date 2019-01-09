package common

import java.io.File

import datatype.{IntDT, TextDT}
import io.IOHandler
import io.model.{BaseColumn, BaseCondition, Page, RecordData}
import query.model.parser.EnumDataType

import scala.collection.mutable
import util.control.Breaks._

object CatalogHelper {

  val TablesTableDatabaseName = 1
  val TablesTableTableName = 2
  val TablesTableRecordCount = 3
  val TablesTableNextColumnTableRowId = 4

  val ColumnsTableDatabaseName = 1
  val ColumnsTableTableName = 2
  val ColumnsTableColumnName = 3
  val ColumnsTableDataType = 4
  val ColumnsTableColumnKey = 5
  val ColumnsTableIsNullable = 6

  val PrimaryKeyPrefix = "PRI"

  def initialiseDatabase = {
    val baseDirectory = new File(Constants.DataDirectoryName)
    if (!baseDirectory.exists()) {
      val catalogDirectory = new File(Constants.DataDirectoryName + "/" + Constants.CatalogDatabaseName)
      if(!catalogDirectory.exists()){
        if(catalogDirectory.mkdir()){
          CatalogHelper.createCatalogDatabase
        }
      }
    }
  }

  def createCatalogDatabase: Boolean = {
    try {
      val handler = new IOHandler()
      handler.createTable(Constants.CatalogDatabaseName, Constants.TobyTables + Constants.DefaultFileExtension)
      handler.createTable(Constants.CatalogDatabaseName, Constants.TobyColumns + Constants.DefaultFileExtension)
      val beginningRowId = updateMetaTables(Constants.CatalogDatabaseName, Constants.TobyTables, 6) *
        updateMetaTables(Constants.CatalogDatabaseName, Constants.TobyColumns, 8)

      if (beginningRowId >= 0) {
        val columns = mutable.MutableList[BaseColumn]()
        columns.+=(BaseColumn(null, "row_id", EnumDataType.Int.toString, false, false)
          , BaseColumn(null, "database_name", EnumDataType.Text.toString, false, false)
          , BaseColumn(null, "table_name", EnumDataType.Text.toString, false, false)
          , BaseColumn(null, "record_count", EnumDataType.Int.toString, false, false)
          , BaseColumn(null, "column_table_st_row_id", EnumDataType.Int.toString, false, false)
          , BaseColumn(null, "next_available_col_row_id", EnumDataType.Int.toString, false, false))
        updateColumnTable(Constants.CatalogDatabaseName, Constants.TobyTables, 1, columns)
        columns.clear()
        columns.+=(BaseColumn(null, "row_id", EnumDataType.Int.toString, false, false)
          , BaseColumn(null, "database_name", EnumDataType.Text.toString, false, false)
          , BaseColumn(null, "table_name", EnumDataType.Text.toString, false, false)
          , BaseColumn(null, "column_name", EnumDataType.Int.toString, false, false)
          , BaseColumn(null, "data_type", EnumDataType.Int.toString, false, false)
          , BaseColumn(null, "column_key", EnumDataType.Int.toString, false, false)
          , BaseColumn(null, "ordinal_position", EnumDataType.Int.toString, false, false)
          , BaseColumn(null, "is_nullable", EnumDataType.Int.toString, false, false))
        updateColumnTable(Constants.CatalogDatabaseName, Constants.TobyColumns, 7, columns)
      }
      return true
    } catch {
      case _: Exception => println("Exception")
    }
    false
  }

  def updateMetaTables(database: String, table: String, columnCount: Int): Int = {
    val handler = new IOHandler()
    val conditionList = mutable.MutableList[BaseCondition]()
    conditionList.+=(BaseCondition(TablesTableTableName, BaseCondition.Equals.toByte, new TextDT(table)))
    conditionList.+=(BaseCondition(TablesTableDatabaseName, BaseCondition.Equals.toByte, new TextDT(table)))
    val result: mutable.MutableList[RecordData] = handler.findRecord(Constants.CatalogDatabaseName, Constants.TobyTables, conditionList, true)

    (result != null, result.isEmpty) match {
      case (true, true) =>
        var returnValue = 1
        val page: Page[RecordData] = handler.getLastRecordAndPage(Constants.CatalogDatabaseName, Constants.TobyTables)
        val finalRecord = page.records.headOption
        val rec = new RecordData()
        finalRecord match {
          case Some(r) => rec.rowId = r.rowId + 1
          case None => rec.rowId = 1
        }

        rec.columnValues.+=(new IntDT(rec.rowId, false), new TextDT(database), new TextDT(table), new IntDT(0, false))
        finalRecord match {
          case Some(r) =>
            val beginColIndex = r.columnValues(TablesTableNextColumnTableRowId).asInstanceOf[IntDT]
            returnValue = beginColIndex.value
            rec.columnValues.+=(new IntDT(returnValue, false), new IntDT(returnValue + columnCount, false))
          case None =>
            rec.columnValues.+=(new IntDT(1, false), new IntDT(1 + columnCount, false))
        }

        rec.populateSize
        handler.writeRecord(Constants.CatalogDatabaseName, Constants.TobyTables, rec)
        returnValue
      case (_, _) =>
        println(s"Table '$database.$table' already exists.")
        -1
    }
  }

  def updateColumnTable(database: String, table: String, beg: Int, columns: mutable.MutableList[BaseColumn]): Boolean = {
    val handler = new IOHandler
    if (columns != null && columns.isEmpty) return false
    var count = 0
    columns.foreach { col =>
      val rec = new RecordData
      rec.rowId = beg + 1
      rec.columnValues.+=(new IntDT(rec.rowId, false), new TextDT(database), new TextDT(table)
        , new TextDT(col.name), new TextDT(col.dataType), new TextDT(col.getStringAsPrimary), new IntDT(count + 1, false)
        , new TextDT(col.getStringAsNullable))
      rec.populateSize
      if (handler.writeRecord(database, Constants.TobyColumns, rec) == -1) break()
      count += 1
    }
    true
  }

  def updateRowCount(database: String, table: String, count: Int) : Int = {
    val IOHandler = new IOHandler()
    val conditionList = mutable.MutableList[BaseCondition]()
    conditionList.+=(BaseCondition(TablesTableTableName, BaseCondition.Equals.toByte, new TextDT(table)))
    conditionList.+=(BaseCondition(TablesTableDatabaseName, BaseCondition.Equals.toByte, new TextDT(table)))

    val updateIndexes = List[Byte](CatalogHelper.TablesTableRecordCount.toByte)
    val updateValues = mutable.MutableList[Object](new IntDT[Int](count, false))
    IOHandler.updateRecord(Constants.CatalogDatabaseName, Constants.TobyTables, conditionList
      , updateIndexes, updateValues, true)
  }

  def incRowCount(database: String, table: String) = updateRowCount(database, table, 1)
  def decRowCount(database: String, table: String) = updateRowCount(database, table, -1)
}
