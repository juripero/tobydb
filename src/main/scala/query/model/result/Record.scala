package query.model.result

import query.model.parser.Literal

import scala.collection.mutable

case class Record(var valueMap: mutable.Map[String, Literal] = mutable.Map[String, Literal]()) {

  def insert(colName: String, value: Literal) = {
    valueMap(colName) = value
  }

  def fetch(colName: String) = valueMap(colName).toString

}
