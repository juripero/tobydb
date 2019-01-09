package datatype.base

abstract class DTypeNum[T] extends DType[T] {

  var size : Byte = _

  def this(valCode: Byte, nullCode: Byte, s: Int) {
    this(valCode, nullCode)
    size = s.toByte
  }

  def getSerialCode =
    isNull match {
      case true => nullSerialCode
      case false => valueSerialCode
    }

  def getSize = size
  def increment(value: T) : Unit
  def compare(obj: DTypeNum[T], condition: Short) : Boolean

}

object DTypeNum {
  val Equals = 0
  val LessThan = 1
  val GreaterThan = 2
  val LessThanEquals = 3
  val GreaterThanEquals = 4
}