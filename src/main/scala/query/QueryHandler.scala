package query

import common.Constants
import query.base.InputQuery
import query.ddl._
import query.dml.{Delete, Insert, Update}
import query.model.parser._
import query.vdl.{DescribeTable, Select, UseDatabase}

import scala.collection.mutable

object QueryHandler {

  val SelectCommand = "SELECT"
  val DropTableCommand = "DROP TABLE"
  val DropDatabaseCommand = "DROP DATABASE"
  val HelpCommand = "HELP"
  val VersionCommand = "VERSION"
  val ExitCommand = "EXIT"
  val ShowTablesCommand = "SHOW TABLES"
  val ShowDatabasesCommand = "SHOW DATABASES"
  val InsertCommand = "INSERT INTO"
  val DeleteCommand = "DELETE FROM"
  val UpdateCommand = "UPDATE"
  val CreateTableCommand = "CREATE TABLE"
  val CreateDatabaseCommand = "CREATE DATABASE"
  val UseDatabaseCommand = "USE"
  val DescTableCommand = "DESC"
  val NoDatabaseSelectedMessage = "No Database Selected"
  val UseHelpMessage = "\nType 'help;' to display supported commands."

  var activeDatabase = ""

  def getVersion = Constants.Version
  def line(s: String, count: Int) = s * count
  def incorrectCommand(cmd: String, msg: String) = {
    println("Invalid Command : " + cmd)
    println("Message : " + msg)
  }

  def help  = {
    println(line("*", 80))
    println("SUPPORTED COMMANDS")
    println("All commands below are case insensitive")
    println
    println("\tUSE DATABASE database_name;                      Changes current database.")
    println("\tCREATE DATABASE database_name;                   Creates an empty database.")
    println("\tSHOW DATABASES;                                  Displays all databases.")
    println("\tDROP DATABASE database_name;                     Deletes a database.")
    println("\tSHOW TABLES;                                     Displays all tables in current database.")
    println("\tDESC table_name;                                 Displays table schema.")
    println("\tCREATE TABLE table_name (                        Creates a table in current database.")
    println("\t\t<column_name> <data_type> [PRIMARY KEY | NOT NULL]")
    println("\t\t...);")
    println("\tDROP TABLE table_name;                           Deletes a table data and its schema.")
    println("\tSELECT <column_list> FROM table_name             Display records whose row_id is <id>.")
    println("\t\t[WHERE row_id = <value>];")
    println("\tINSERT INTO table_name                           Inserts a record into the table.")
    println("\t\t[(<column1>, ...)] VALUES (<value1>, <value2>, ...);")
    println("\tDELETE FROM table_name [WHERE condition];        Deletes a record from a table.")
    println("\tUPDATE table_name SET <conditions>               Updates a record from a table.")
    println("\t\t[WHERE condition];")
    println("\tVERSION;                                         Display current database engine version.")
    println("\tHELP;                                            Displays help information")
    println("\tEXIT;                                            Exits the program")
    println
    println
  }

  def showTable = new ShowTable(activeDatabase)
  def dropTable(table: String) = DropTable(activeDatabase, table)

  def selectTable(table: String, attr : mutable.MutableList[String], conditionString: String) : InputQuery = {
    if(QueryHandler.activeDatabase.equals("")){
      println("No db selected")
      return null
    }

    var selectAll = false
    var columns = mutable.MutableList[String]()
    attr.foreach(a => columns.+=(a.trim))

    if(columns.size == 1 && columns.head.equals("*")){
      selectAll = true
      columns = null
    }

    if(conditionString.equals("")) Select(activeDatabase, table, columns, null)
    else{
      val condList = mutable.MutableList[Condition](Condition.createCondition(conditionString))
      Select(activeDatabase, table, columns, condList)
    }
  }

  def insertTable(table: String, column: String, values: String) : InputQuery = {
    if(QueryHandler.activeDatabase.equals("")){
      println("No db selected")
      return null
    }

    var colList : mutable.MutableList[String] = null
    if(!column.equals("")){
      colList = mutable.MutableList[String]()
      column.split(",").map(e => colList.+=(e.trim))
    }

    val literals = mutable.MutableList[Literal]()
    values.split(",").foreach { e =>
      val lit = Literal.createLiteral(e)
      if(e != null) literals.+=(lit) else return null
    }

    if(colList != null && colList.size != literals.size){
      println("Column and Value count do not match")
      null
    }else
      Insert(activeDatabase, table, literals, colList)
  }

  def deleteTable(table: String, condition: String) : InputQuery = {
    if(QueryHandler.activeDatabase.equals("")){
      println("No db selected")
      return null
    }

    if(condition.equals("")) return Delete(activeDatabase, table, null, false)
    val conditionList = mutable.MutableList[Condition](Condition.createCondition(condition))
    Delete(activeDatabase, table, conditionList, false)
  }

  def updateTable(table: String, clause: String, condition: String) : InputQuery = {
    if(QueryHandler.activeDatabase.equals("")){
      println("No db selected")
      return null
    }

    val cl = Condition.createCondition(clause)
    if(cl == null) return null

    if(cl.operator != Operator.Equals){
      println("Update should only contain equality operator")
      return null
    }

    if(condition == ""){
      return Update(activeDatabase, table, cl.column, cl.literal, null)
    }

    val con = Condition.createCondition(condition)
    if(con == null) null
    else Update(activeDatabase, table, cl.column, cl.literal, con)
  }

  def createTable(table: String, columnString: String) : InputQuery = {
    if(activeDatabase.equals("")){
      println("No db selected")
      return null
    }

    val colList = columnString.split(",")
    val columns = mutable.MutableList[Column]()

    colList.foreach { c =>
      val col = Column.createColumn(c)
      if(col == null) return null
      columns.+=(col)
    }

    var hasPk = false
    (0 to colList.length).foreach { i =>
      if(colList(i).toLowerCase.endsWith("primary key")){
        if(i == 0){
          if(columns(i).dType == EnumDataType.Int) hasPk = true
          else {
            println("PK has to be INT")
            return null
          }
        }else{
          println("Only first column can be PK")
          return null
        }
      }
    }

    CreateTable(activeDatabase, table, columns, hasPk)
  }

  def createDB(database: String) = CreateDatabase(database)
  def useDB(database: String) = UseDatabase(database)
  def dropDB(database: String) = DropDatabase(database)
  def showDB(database: String) = new ShowDatabase

  def descTable(table: String) : DescribeTable = {
    if(activeDatabase.equals("")){
      println("No db selected")
      return null
    }
    DescribeTable(activeDatabase, table)
  }

  def executeQuery(query: InputQuery) = {
    if(query != null && query.validateQuery){
      val result = query.executeQuery
      if(result != null) result.show
    }
  }

}