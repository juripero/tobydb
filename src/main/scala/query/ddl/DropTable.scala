package query.ddl

import java.io.File

import common.{Constants, DBHelper, Utility}
import io.IOHandler
import query.QueryHandler
import query.base.InputQuery
import query.dml.Delete
import query.model.parser.Condition
import query.model.result.Result

import scala.collection.mutable

case class DropTable(database: String, table: String) extends InputQuery {

  override def validateQuery: Boolean = {
    val dbExist = new DBHelper(new IOHandler).doesTableExist(database, table)
    if(!dbExist){
      println(s"$database.$table does not exist")
      return false
    }
    true
  }

  override def executeQuery: Result = {
    val db = new File(Utility.getDatabasePath(database))
    val conditionList = mutable.MutableList[Condition](Condition.createCondition(s"database_name = $database"),
      Condition.createCondition(s"table_name = $table"))

    var deleteQuery = Delete(Constants.CatalogDatabaseName, Constants.TobyTables, conditionList, true)
    deleteQuery.executeQuery

    deleteQuery = Delete(Constants.CatalogDatabaseName, Constants.TobyColumns, conditionList, true)
    deleteQuery.executeQuery

    val isDeleted = Utility.recursiveDelete(db)
    if(!isDeleted){
      println(s"Unable to delete $database folder")
      return null
    }

    if(QueryHandler.activeDatabase.equals(database)) QueryHandler.activeDatabase = ""
    new Result(1)
  }

}
