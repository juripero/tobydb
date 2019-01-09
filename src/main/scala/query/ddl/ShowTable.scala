package query.ddl

import common.{Constants, DBHelper}
import io.IOHandler
import query.base.InputQuery
import query.model.parser.Condition
import query.model.result.Result
import query.vdl.Select

import scala.collection.mutable

class ShowTable(var database: String) extends InputQuery {

  override def executeQuery: Result = {
    val columns = mutable.MutableList("table_name")
    val conditionList = mutable.MutableList[Condition](Condition.createCondition(s"database_name = '$database'"))

    val query = Select(Constants.CatalogDatabaseName, Constants.TobyTables, columns, conditionList)
    if(query.validateQuery) return query.executeQuery
    null
  }

  override def validateQuery: Boolean = {
    val dbExist = new DBHelper(new IOHandler).doesDBExist(database)
    if(!dbExist){
      println(s"$database does not exist")
      return false
    }
    true
  }
}
