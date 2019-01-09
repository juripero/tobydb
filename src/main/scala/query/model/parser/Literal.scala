package query.model.parser

import datatype.base.DType
import common.{Constants, Utility}

import scala.util.Try

class Literal(var dataType: EnumDataType.Value, var value: String) {

  def createLiteral(v: DType[_], dataType: Byte) = {
    dataType match {
      case Constants.BigInt => new Literal(EnumDataType.BigInt, v.fetchString)
    }
  }

  def createLiteral(literalString: String) : Literal = {
    null
  }

  override def toString: String = {
    dataType match {
      case EnumDataType.BigInt || EnumDataType.Text || EnumDataType.Int || EnumDataType.TinyInt
        || EnumDataType.SmallInt || EnumDataType.Date || EnumDataType.DateTime => value
      case EnumDataType.Real || EnumDataType.Double => String.format("%.2f", value.toDouble)
      case EnumDataType.IntNull || EnumDataType.SmallIntNull || EnumDataType.TinyIntNull
        || EnumDataType.DoubleDateTimeNull => "NULL"
      case _ => ""
    }
  }

}

object Literal {

  def createLiteral(literalString: String) : Literal = {
    if(literalString.startsWith("'") && literalString.endsWith("'")){
      val tmp = literalString.substring(1, literalString.length - 1)
      if(Utility.validateDateTimeFormat(tmp)) return new Literal(EnumDataType.DateTime, tmp)
      if(Utility.validateDateTimeFormat(tmp)) return new Literal(EnumDataType.Date, tmp)
      return new Literal(EnumDataType.Text, tmp)
    }

    if(literalString.startsWith("\"") && literalString.endsWith("\"")){
      val tmp = literalString.substring(1, literalString.length - 1)
      if(Utility.validateDateTimeFormat(tmp)) return new Literal(EnumDataType.DateTime, tmp)
      if(Utility.validateDateTimeFormat(tmp)) return new Literal(EnumDataType.Date, tmp)
      return new Literal(EnumDataType.Text, tmp)
    }

    if(Try(literalString.toInt).isSuccess) return new Literal(EnumDataType.Int, literalString)
    if(Try(literalString.toLong).isSuccess) return new Literal(EnumDataType.BigInt, literalString)
    if(Try(literalString.toDouble).isSuccess) return new Literal(EnumDataType.Double, literalString)
    null
  }

  def createLiteral(dType: DType[_], t: Byte) : Literal = {
    if(t == Constants.InvalidClass) return null
    if(dType.isNull) return new Literal(EnumDataType.DoubleDateTimeNull, dType.fetchString)

    t match {
      case Constants.TinyInt => new Literal(EnumDataType.TinyInt, dType.fetchString)
      case Constants.SmallInt => new Literal(EnumDataType.SmallInt, dType.fetchString)
      case Constants.Int => new Literal(EnumDataType.Int, dType.fetchString)
      case Constants.BigInt => new Literal(EnumDataType.BigInt, dType.fetchString)
      case Constants.Real => new Literal(EnumDataType.Real, dType.fetchString)
      case Constants.Double => new Literal(EnumDataType.Double, dType.fetchString)
      case Constants.Date => new Literal(EnumDataType.Date, dType.fetchString)
      case Constants.DateTime => new Literal(EnumDataType.DateTime, dType.fetchString)
      case Constants.Text => new Literal(EnumDataType.Text, dType.fetchString)
    }
  }
}
