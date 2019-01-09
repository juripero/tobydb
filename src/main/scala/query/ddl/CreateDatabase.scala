package query.ddl

import java.io.File

import common.{DBHelper, Utility}
import io.IOHandler
import query.base.InputQuery
import query.model.result.Result

case class CreateDatabase(database: String) extends InputQuery {

  override def executeQuery: Result = {
    val file = new File(Utility.getDatabasePath(database))
    val created = file.mkdir()

    if(!created){
      println(s"Unable to create $database")
      return null
    }

    new Result(1)
  }

  override def validateQuery: Boolean = {
    val dbExist = new DBHelper(new IOHandler).doesDBExist(database)
    if(dbExist){
      println(s"Database $database already exist")
      return false
    }
    true
  }

}
