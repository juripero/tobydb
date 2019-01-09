package query.ddl

import java.io.File

import common.Constants
import query.base.InputQuery
import query.model.parser.Literal
import query.model.result.{Record, Result, ResultSet}

import scala.collection.mutable

class ShowDatabase extends InputQuery {

  override def executeQuery: Result = {
    val colList = mutable.MutableList[String]("Database")
    val resultSet = ResultSet.createResultSet
    resultSet.columns = colList
    val records = getDbs
    records.foreach(resultSet.addRecord)
    resultSet
  }

  override def validateQuery: Boolean = true

  def getDbs = {
    val records = mutable.MutableList[Record]()
    val file = new File(Constants.DataDirectoryName)
    file.listFiles().foreach { f =>
      if(f.isDirectory){
        val record = Record()
        record.insert("Database", Literal.createLiteral(s"\"${f.getName}\""))
        records.+=(record)
      }
    }
    records
  }

}
