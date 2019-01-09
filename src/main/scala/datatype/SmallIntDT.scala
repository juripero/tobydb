package datatype

import datatype.base.DTypeNum
import common.Constants
import datatype.base.DTypeNum._

class SmallIntDT[T <: Short] extends DTypeNum[Short] {

  def this(v: Short, isN: Boolean) {
    this(Constants.RealSerialTypeCode, Constants.FourByteNullSerialTypeCode, java.lang.Short.BYTES)
    value = v
    isNull = isN
  }

  override def increment(v: Short): Unit = {
    value += v
  }

  override def compare(obj: DTypeNum[Short], condition: Short): Boolean =
    condition match {
      case Equals => value == obj.value
      case GreaterThan => value > obj.value
      case LessThan => value < obj.value
      case GreaterThanEquals => value >= obj.value
      case LessThanEquals => value <= obj.value
      case _ => false
    }

  def compare(obj: TinyIntDT[Byte], condition: Short) = {
    val ob = new SmallIntDT[Byte](obj.value, false)
    compare(ob, condition)
  }

  def compare(obj: IntDT[Int], condition: Short) = {
    val ob = new IntDT[Int](value, false)
    ob.compare(obj.asInstanceOf[IntDT], condition)
  }

  def compare(obj: BigIntDT[Long], condition: Short) = {
    val ob = new BigIntDT[Long](value, false)
    ob.compare(obj.asInstanceOf[BigIntDT], condition)
  }

}

