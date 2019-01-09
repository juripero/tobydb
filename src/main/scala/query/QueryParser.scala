package query
import QueryHandler._
import query.ddl.ShowDatabase

import scala.collection.mutable

object QueryParser {

  var stop = false

  def parseUserCommand(cmd: String) : Unit = {
    if(cmd.equals(ShowTablesCommand)) executeQuery(showTable)
    else if(cmd.equals(ShowDatabasesCommand)) executeQuery(new ShowDatabase())
    else if(cmd.equals(HelpCommand)) help
    else if(cmd.equals(VersionCommand)) getVersion
    else if(cmd.equals(ExitCommand)) {
      println("Closing db")
      stop = true
    }
    else if(cmd.startsWith(UseDatabaseCommand)){
      if(!cmd.equals(UseDatabaseCommand)) return
      val db = cmd.substring(UseDatabaseCommand.length)
      executeQuery(useDB(db))
    }
    else if(cmd.startsWith(DescTableCommand)){
      if(!cmd.equals(DescTableCommand)) return
      val table = cmd.substring(DescTableCommand.length)
      executeQuery(descTable(table))
    }
    else if(cmd.startsWith(DropTableCommand)){
      if(!cmd.equals(DropTableCommand)) return
      val table = cmd.substring(DropTableCommand.length)
      executeQuery(dropTable(table))
    }
    else if(cmd.startsWith(DropDatabaseCommand)){
      if(!cmd.equals(DropDatabaseCommand)) return
      val db = cmd.substring(DropDatabaseCommand.length)
      executeQuery(dropDB(db))
    }
    else if(cmd.startsWith(SelectCommand)){
      if(!cmd.equals(SelectCommand)) return
      val ind = cmd.indexOf("from")
      if(ind == -1) {
        println("Incorrect Query")
        return
      }

      val attrList = mutable.MutableList[String]()
      cmd.substring(SelectCommand.length, ind).trim.split(",").foreach(e => attrList.+=(e))
      val remQ = cmd.substring(ind + "from".length)
      val index = cmd.indexOf("where")
      if(index == -1){
        executeQuery(selectTable(remQ.trim, attrList, ""))
        return
      }

      executeQuery(selectTable(remQ.substring(0, index), attrList, remQ.substring(index + "where".length)))
    }
    else if(cmd.startsWith(InsertCommand)){
      if(!cmd.equals(InsertCommand)) return
      val indexOfVal = cmd.indexOf("values")

      if(indexOfVal == -1){
        println("Incorrect command")
        return
      }

      var table : String = null
      var columns : String = null
      val openBracket = cmd.indexOf("(")

      if(openBracket != -1){
        table = cmd.substring(InsertCommand.length, openBracket).trim
        if(cmd.indexOf(")") == -1){
          println("Incorrect Command")
          return
        }
        columns = cmd.substring(openBracket + 1, cmd.indexOf(")"))
      }

      if(table == null) table = cmd.substring(InsertCommand.length, indexOfVal).trim
      val listOfValues = cmd.substring(indexOfVal + "values".length).trim.substring(1, -1)
      insertTable(table, columns, listOfValues)
    }
    else if(cmd.startsWith(DeleteCommand)){
      if(!cmd.equals(DeleteCommand)) return
      var table: String = ""
      if(cmd.contains("where")){
        executeQuery(deleteTable(cmd.substring(DeleteCommand.length).trim, ""))
        return
      }

      if(table != "") table = cmd.substring(DeleteCommand.length, cmd.indexOf("where")).trim
      deleteTable(table, cmd.substring(cmd.indexOf("where") + "where".length))
    }
    else if(cmd.startsWith(UpdateCommand)){
      if(!cmd.equals(UpdateCommand)) return
      val setIndex = cmd.indexOf("set")
      if(setIndex == -1){
        println("Incorrect Command")
        return
      }
      val table = cmd.substring(UpdateCommand.length, setIndex).trim
      val clauses = cmd.substring(setIndex + "set".length)
      val whereIndex = cmd.indexOf("where")
      if(whereIndex == -1){
        executeQuery(updateTable(table, clauses, ""))
        return
      }

      executeQuery(updateTable(table, cmd.substring(setIndex + "set".length, whereIndex).trim
        , cmd.substring(whereIndex + "where".length).trim))
    }
    else if(cmd.startsWith(CreateDatabaseCommand)){
      if(!cmd.equals(CreateDatabaseCommand)) return
      executeQuery(createDB(cmd.substring(CreateDatabaseCommand.length).trim))
    }
    else if(cmd.startsWith(CreateTableCommand)){
      if(!cmd.equals(CreateTableCommand)) return
      val openBracket = cmd.indexOf("(")
      if(openBracket == -1 || !cmd.endsWith(")")){
        println("Incorrect Command")
        return
      }
      createTable(cmd.substring(CreateTableCommand.length, openBracket-1), cmd.substring(openBracket+1, cmd.length-1))
    }
    else
      println(HelpCommand)
  }

}
