package query.vdl

import common.{CatalogHelper, Constants, DBHelper, Utility}
import datatype.TextDT
import datatype.base.DType
import io.IOHandler
import io.model.{BaseCondition, RecordData}
import query.base.InputQuery
import query.model.parser.{Condition, Literal}
import query.model.result.{Record, Result, ResultSet}

import scala.collection.mutable

case class Select(var database: String, var table: String, var columns: mutable.MutableList[String]
                  , conditions: mutable.MutableList[Condition]) extends InputQuery{

  override def executeQuery: Result = {
    val result = ResultSet.createResultSet
    val records = getData
    records.foreach(result.addRecord)
    result
  }

  override def validateQuery: Boolean = {
    val handler = new IOHandler()
    if(!handler.checkIfTableExists(database, table)){
      println(s"$database.$table not found")
      return false
    }

    val maps = mapIdToCol(table)
    val colMap = maps._1.toMap

    val dbHelperObj = new DBHelper(handler)
    val colDT = dbHelperObj.getDataTypesOfAllColumns(database, table, colMap)

    if(conditions != null){
      val rCol = dbHelperObj.fetchColumns(database, table)
      conditions.foreach { c =>
        if(!Utility.isDataTypeOfConditionValid(colDT, rCol, c)) return false
      }
    }

    if(columns != null){
      columns.foreach { c =>
        if(!colMap.contains(c)){
          println(s"Column $c not found in table $table")
          return false
        }
      }
    }

    if(conditions != null){
      conditions.foreach { c =>
        if(!colMap.contains(c.column)){
          println(s"Column ${c.column} not found in table $table")
          return false
        }
      }
    }

    true

  }

  def mapIdToCol(table: String) = {
    val idMap = mutable.HashMap[Int, String]()
    val colMap = mutable.HashMap[String, Int]()
    val conditions = mutable.MutableList[BaseCondition](BaseCondition(CatalogHelper.ColumnsTableTableName,
      BaseCondition.Equals, new TextDT(table)))
    val records = new IOHandler().findRecord(Constants.CatalogDatabaseName, Constants.TobyColumns, conditions, false)

    (0 to records.size).foreach { ind =>
      val rec = records(ind)
      val ob = rec.columnValues(CatalogHelper.ColumnsTableColumnName)
      idMap.put(ind, ob.asInstanceOf[DType].fetchString)
      colMap.put(ob.asInstanceOf[DType].fetchString, ind)
    }
    (colMap, idMap)
  }

  def getData = {
    val records = mutable.MutableList[Record]()
    var internalRecords = mutable.MutableList[RecordData]()
    val mp = mapIdToCol(table)
    val colMap = mp._1
    val colList = mutable.MutableList[Byte]()
    val handler = new IOHandler()
    val conditionList = mutable.MutableList[BaseCondition]()

    if(conditions != null){
      conditions.foreach { c =>
        val internalCon = BaseCondition(colMap(c.column), Utility.numericOperator(c.operator).toShort, DType.createDType(c.literal))
        conditionList.+=(internalCon)
      }
    }

    if(columns == null){
      internalRecords = handler.findRecord(database, table, conditionList, false)
      val idMap = mp._2
      columns = List[String]()
      (0 to colMap.size).foreach { ind =>
        if(idMap.contains(ind)){
          colList.+=(ind.toByte)
          columns.+=(idMap(ind))
        }
      }
    }else{
      columns.foreach { col =>
        if(colMap.contains(col)) colList.+=(colMap(col).toByte)
      }

      internalRecords = handler.findRecord(database, table, conditionList, colList, false)
    }

    val colId = mutable.MutableList[Byte]()
    colList.foreach(colId += _ )

    val idMap = mp._2
    internalRecords.foreach { r =>
      val dataTypes = mutable.MutableList[Object]()
      r.columnValues.foreach(e => dataTypes.+=(e))
      val record = Record(null)
      (0 to colId.size).foreach { ind =>
        if(idMap.contains(colId(ind))){
          val literal = Literal.createLiteral(dataTypes(ind).asInstanceOf[DType[_]], Utility.validateClass(dataTypes(ind)))
          record.insert(idMap(colId(ind)), literal)
        }
      }
      records.+=(record)
    }

    records

  }

}
