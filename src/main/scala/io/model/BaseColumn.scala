package io.model

case class BaseColumn(
     var value: Object = null
     , var name: String
     , var dataType: String
     , var isPri: Boolean
     , var canBeNull: Boolean
     , var position: Byte = 0){

  def getStringAsPrimary = {
    isPri match {
      case true => "PRI"
      case false => null
    }
  }

  def getStringAsNullable = {
    canBeNull match {
      case true => "YES"
      case false => "NO"
    }
  }
}
