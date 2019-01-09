package query.model.result

import java.util

import scala.collection.mutable

class ResultSet(var columns : mutable.MutableList[String], var records : mutable.MutableList[Record]) extends Result {

  def addRecord(rec: Record) = {
    records += rec
    rowsChanged += 1
  }

  override def show: Unit = {

    def max(a: Int, b: Int) =
      a > b match {
        case true => a
        case false => b
      }

    if(columns == null || columns.isEmpty) return

    val columnSize = mutable.Map[String, Int]()
    columns.foreach { col =>
      val mx : Int = max(records.map(_.valueMap.getOrElse(col, "").toString.length).max, col.length)
      columnSize(col) = mx
    }

    println()
    val sep = showLine(columns, columnSize)
    println(sep)
    println(showColumns(columnSize))
    println(sep)

    records.foreach(r => println(showRecord(r, columnSize)))
    println(sep)

    println(s"Number of rows changes -> $rowsChanged\n")

  }

  def showRecord(record: Record, columnSize: mutable.Map[String, Int]) = {
    val buffer = new StringBuffer
    columns.map { col =>
      buffer.append("| ")
      if (record.valueMap.contains(col)) {
        val value = record.fetch(col)
        buffer.append(value)
        buffer.append(fill(' ', columnSize(col) - value.length + 1))
      }
    }
    buffer.append("|")
    buffer.toString
  }

  def showColumns(columnSize: mutable.Map[String, Int]) = {
    val buffer = new StringBuffer
    columns.map { col =>
      buffer.append("| ")
      buffer.append(col)
      buffer.append(fill(' ', columnSize(col) - col.length + 1))
    }
    buffer.append("|")
    buffer.toString
  }

  def showLine(columns: mutable.MutableList[String], columnSize: mutable.Map[String, Int]) = {
    val buffer = new StringBuffer
    columns.map { col =>
      buffer.append("+")
      buffer.append(fill('-', columnSize(col) + 2))
    }

    buffer.append("+")
    buffer.toString
  }

  def fill(character: Char, size: Int): String = {
    val repeatedCharacters = new Array[Char](size)
    util.Arrays.fill(repeatedCharacters, character)
    repeatedCharacters.toString
  }

}

object ResultSet {
  def createResultSet = new ResultSet(mutable.MutableList[String](), mutable.MutableList[Record]())
}
