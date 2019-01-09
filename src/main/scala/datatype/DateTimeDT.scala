package datatype

import java.text.SimpleDateFormat
import java.util.Date

import common.Constants
import datatype.base.DTypeNum
import datatype.base.DTypeNum._

class DateTimeDT[T <: Long] extends DTypeNum[Long] {

  def this(v: Long, isN: Boolean) {
    this(Constants.DateTimeSerialTypeCode, Constants.EightByteNullSerialTypeCode, java.lang.Long.BYTES)
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

  override def toString: String = {
    val dt = new Date(value)
    return new SimpleDateFormat("MM-dd-yyyy : HH:mm:ss").format(dt)
  }

}
