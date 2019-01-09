package query.ddl

import common.{CatalogHelper, Constants}
import io.IOHandler
import io.model.BaseColumn
import query.base.InputQuery
import query.model.parser.Column
import query.model.result.Result

import scala.collection.mutable

case class CreateTable(database: String, table: String, columns: mutable.MutableList[Column], primaryKey: Boolean) extends InputQuery {

  override def executeQuery: Result = new Result(1)

  override def validateQuery: Boolean = {
    val handler = new IOHandler
    if(!handler.checkIfDbExists(database)){
      println(s"$database does not exist")
      return false
    }

    if(handler.checkIfTableExists(database, table)){
      println(s"$database.$table does not exist")
      return false
    }

    if(duplicateColumnsPresent(columns)){
      println(s"Duplicate Columns Present")
      return false
    }

    val colList = mutable.MutableList[BaseColumn]()
    (0 to columns.size).foreach { ind =>
      val internalCol = BaseColumn(null, columns(ind).name, columns(ind).dType.toString, false, false)
      if(primaryKey && ind == 0) {
        internalCol.isPri = true
        internalCol.canBeNull = false
      } else internalCol.isPri = false

      if(columns(ind).isNull) internalCol.canBeNull = true
      colList.+=(internalCol)
    }

    val status = new IOHandler().createTable(database, table + Constants.DefaultFileExtension)
    if(status){
      val startRowId = CatalogHelper.updateMetaTables(database, table, columns.size)
      val tableUpdateStatus = CatalogHelper.updateColumnTable(database, table, startRowId, colList)
      if(!tableUpdateStatus){
        println(s"Failed to create table $table")
        return false
      }
    }
    true
  }

  def duplicateColumnsPresent(col: mutable.MutableList[Column]) : Boolean = {
    val set = mutable.HashSet[String]()
    col.foreach { c =>
      if(set.contains(c.name))return true
      else set.add(c.name)
    }
    false
  }
}
