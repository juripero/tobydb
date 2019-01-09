package datatype

import common.Constants
import datatype.base.DTypeNum
import datatype.base.DTypeNum._

class DoubleDT[T <: Double] extends DTypeNum[Double] {

  def this(v: Double, isN: Boolean) {
    this(Constants.DoubleSerialTypeCode, Constants.EightByteNullSerialTypeCode, java.lang.Double.BYTES)
    value = v
    isNull = isN
  }

  override def increment(v: Double): Unit = {
    value += v
  }

  override def compare(obj: DTypeNum[Double], condition: Short): Boolean =
    condition match {
      case Equals => java.lang.Double.doubleToLongBits(value) == java.lang.Double.doubleToLongBits(obj.value)
      case GreaterThan => value > obj.value
      case LessThan => value < obj.value
      case GreaterThanEquals => java.lang.Double.doubleToLongBits(value) >= java.lang.Double.doubleToLongBits(obj.value)
      case LessThanEquals => java.lang.Double.doubleToLongBits(value) <= java.lang.Double.doubleToLongBits(obj.value)
      case _ => false
    }

  def compare(obj: RealDT[Float], condition: Short) = {
    val ob = new DoubleDT[Double](obj.value, false)
    compare(ob, condition)
  }

}
