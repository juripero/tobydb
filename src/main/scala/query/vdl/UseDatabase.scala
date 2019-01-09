package query.vdl

import common.DBHelper
import io.IOHandler
import query.QueryHandler
import query.base.InputQuery
import query.model.result.Result

case class UseDatabase(database: String) extends InputQuery {

  override def executeQuery: Result = {
    QueryHandler.activeDatabase = database
    println("db changed")
    null
  }

  override def validateQuery: Boolean = {
    val tableExist = new DBHelper(new IOHandler).doesDBExist(database)
    if(!tableExist){
      println(s"$database does not exist")
      return false
    }
    true
  }

}
