package datatype

import datatype.base.DTypeNum
import common.Constants
import datatype.base.DTypeNum._

class BigIntDT[T <: Long] extends DTypeNum[Long] {

  def this(v: Long, isN: Boolean) {
    this(Constants.BigIntSerialTypeCode, Constants.EightByteNullSerialTypeCode, java.lang.Long.BYTES)
    value = v
    isNull = isN
  }

  override def increment(v: Long): Unit = {
    value += v
  }

  override def compare(obj: DTypeNum[Long], condition: Short): Boolean =
    condition match {
      case Equals => value == obj.value
      case GreaterThan => value > obj.value
      case LessThan => value < obj.value
      case GreaterThanEquals => value >= obj.value
      case LessThanEquals => value <= obj.value
      case _ => false
    }

  def compare(obj: SmallIntDT[Short], condition: Short) = {
    val ob = new BigIntDT[Long](obj.value, false)
    compare(ob, condition)
  }

  def compare(obj: TinyIntDT[Byte], condition: Short) = {
    val ob = new BigIntDT[Long](obj.value, false)
    compare(ob, condition)
  }

  def compare(obj: IntDT[Int], condition: Short) = {
    val ob = new BigIntDT[Long](obj.value, false)
    compare(ob, condition)
  }

}
