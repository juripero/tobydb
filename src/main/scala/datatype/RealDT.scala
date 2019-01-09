package datatype

import datatype.base.DTypeNum
import common.Constants
import datatype.base.DTypeNum._

class RealDT[T <: Float] extends DTypeNum[Float] {

  def this(v: Float, isN: Boolean) {
    this(Constants.RealSerialTypeCode, Constants.FourByteNullSerialTypeCode, java.lang.Float.BYTES)
    value = v
    isNull = isN
  }

  override def increment(v: Float): Unit = {
    value += v
  }

  override def compare(obj: DTypeNum[Float], condition: Short): Boolean =
    condition match {
      case Equals => java.lang.Float.floatToIntBits(value) == java.lang.Float.floatToIntBits(obj.value)
      case GreaterThan => value > obj.value
      case LessThan => value < obj.value
      case GreaterThanEquals => java.lang.Float.floatToIntBits(value) >= java.lang.Float.floatToIntBits(obj.value)
      case LessThanEquals => java.lang.Float.floatToIntBits(value) <= java.lang.Float.floatToIntBits(obj.value)
      case _ => false
    }

  def compare(obj: DoubleDT[Double], condition: Short) = {
    val ob = new DoubleDT[Double](value, false)
    ob.compare(obj, condition)
  }

}
