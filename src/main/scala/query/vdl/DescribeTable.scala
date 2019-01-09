package query.vdl

import common.{Constants, DBHelper}
import io.IOHandler
import query.base.InputQuery
import query.model.parser.Condition
import query.model.result.Result

import scala.collection.mutable

case class DescribeTable(database: String, table: String) extends InputQuery{

  override def executeQuery: Result = {
    val colList = mutable.MutableList[String]()
    colList.+=("column_name", "data_type", "column_key", "is_nullable")

    val conditionList = mutable.MutableList[Condition]()
    conditionList.+=(Condition.createCondition(s"database_name = $database"),
      Condition.createCondition(s"table_name = $table"))

    val query = new Select(Constants.CatalogDatabaseName, Constants.TobyColumns, colList, conditionList, false)
    if(query.validateQuery) return query.executeQuery
    null
  }

  override def validateQuery: Boolean = {
    val tableExist = new DBHelper(new IOHandler).doesTableExist(database, table)
    if(!tableExist){
      println(s"$database.$table does not exist")
      return false
    }
    true
  }
}
