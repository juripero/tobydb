package datatype.base

import query.model.parser.{EnumDataType, Literal}
import datatype._

abstract class DType[T <: Object] {

  var value: T = _
  var isNull: Boolean = _
  var valueSerialCode: Byte = _
  var nullSerialCode: Byte = _

  def this(valCode: Byte, nullCode: Byte) {
    this()
    this.valueSerialCode = valCode
    this.nullSerialCode = nullCode
  }

  def fetchString = {
    value == null match {
      case true => "NULL"
      case false => value.toString
    }
  }

  def setValue(v : T) = {
    value = v
    isNull = value == v
  }

}

object DType extends EnumDataType {

  def createDType(v: Literal) : DType[_] =
    v.dataType match {
      case TinyInt => new TinyIntDT[Byte](v.value.toByte, false)
      case SmallInt => new SmallIntDT[Short](v.value.toShort, false)
      case Int => new IntDT[Int](v.value.toInt, false)
      case BigInt => new BigIntDT[Long](v.value.toLong, false)
      case Real => new RealDT[Float](v.value.toFloat, false)
      case Double => new DoubleDT[Double](v.value.toDouble, false)
      case Date => new DateDT[Long](v.value.toLong, false)
      case DateTime => new DateTimeDT[Long](v.value.toLong, false)
      case Text => new TextDT(v.value, false)
    }

  def createSystemDType(v: String, dType: Byte) =
    dType match {
      case TinyInt => new TinyIntDT[Byte](v.toByte, false)
      case SmallInt => new SmallIntDT[Short](v.toShort, false)
      case Int => new IntDT[Int](v.toInt, false)
      case BigInt => new BigIntDT[Long](v.toLong, false)
      case Real => new RealDT[Float](v.toFloat, false)
      case Double => new DoubleDT[Double](v.toDouble, false)
      case Date => new DateDT[Long](v.toLong, false)
      case DateTime => new DateTimeDT[Long](v.toLong, false)
      case Text => new TextDT(v, false)
    }

}