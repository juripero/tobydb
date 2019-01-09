package datatype

import datatype.base.DTypeNum
import common.Constants
import datatype.base.DTypeNum._

class IntDT[T <: Int] extends DTypeNum[Int] {

  def this(v: Int, isN: Boolean) {
    this(Constants.IntSerialTypeCode, Constants.FourByteNullSerialTypeCode, java.lang.Integer.BYTES)
    value = v
    isNull = isN
  }

  override def increment(v: Int): Unit = {
    value += v
  }

  override def compare(obj: DTypeNum[Int], condition: Short): Boolean =
    condition match {
      case Equals => value == obj.value
      case GreaterThan => value > obj.value
      case LessThan => value < obj.value
      case GreaterThanEquals => value >= obj.value
      case LessThanEquals => value <= obj.value
      case _ => false
    }

  def compare(obj: TinyIntDT[Byte], condition: Short) = {
    val ob = new IntDT[Byte](obj.value, false)
    compare(ob, condition)
  }

  def compare(obj: SmallIntDT[Short], condition: Short) = {
    val ob = new IntDT[Int](obj.value, false)
    compare(ob, condition)
  }

  def compare(obj: BigIntDT[Long], condition: Short) = {
    val ob = new BigIntDT[Long](value, false)
    ob.compare(obj.asInstanceOf[BigIntDT], condition)
  }

}
