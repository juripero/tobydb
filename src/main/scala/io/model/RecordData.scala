package io.model

import common.{Constants, Utility}
import datatype._

import scala.collection.mutable

case class RecordData(
   var columnValues: mutable.MutableList[Object] = mutable.MutableList[Object]()
   , var size: Short = 0
   , var rowId: Int = 0
   , var page: Int = -1
   , var offset: Short = -1){

  def populateSize = {
    size = (columnValues.size + 1).toShort
    columnValues.foreach { ob =>
      val tmp = ob match {
        case a : TinyIntDT[Byte] => a.getSize
        case a : SmallIntDT[Short] => a.getSize
        case a : IntDT[Int] => a.getSize
        case a : BigIntDT[Long] => a.getSize
        case a : RealDT[Float] => a.getSize
        case a : DoubleDT[Double] => a.getSize
        case a : DateDT[Long] => a.getSize
        case a : DateTimeDT[Long] => a.getSize
        case a : TextDT => a.getSize
      }
      size += tmp
    }
  }

  def headerSize = java.lang.Short.BYTES + java.lang.Integer.BYTES

  def getSerialTypeCodes = {
    val typeCode = mutable.MutableList[Byte]()
    columnValues.foreach { c =>
      Utility.validateClass(c) match {
        case Constants.TinyInt => typeCode.+=(c.asInstanceOf[TinyIntDT].getSerialCode)
        case Constants.SmallInt => typeCode.+=(c.asInstanceOf[SmallIntDT].getSerialCode)
        case Constants.Int => typeCode.+=(c.asInstanceOf[IntDT].getSerialCode)
        case Constants.BigInt => typeCode.+=(c.asInstanceOf[BigIntDT].getSerialCode)
        case Constants.Real => typeCode.+=(c.asInstanceOf[RealDT].getSerialCode)
        case Constants.Double => typeCode.+=(c.asInstanceOf[DoubleDT].getSerialCode)
        case Constants.Date => typeCode.+=(c.asInstanceOf[DateDT].getSerialCode)
        case Constants.DateTime => typeCode.+=(c.asInstanceOf[DateTimeDT].getSerialCode)
        case Constants.Text => typeCode.+=(c.asInstanceOf[TextDT].getSerialCode)
      }
    }
    typeCode.toList
  }
}
