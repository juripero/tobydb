package datatype

import common.Constants
import datatype.base.DType

class TextDT extends DType[String]{

  def this(v: String, isN: Boolean = null) {
    this(Constants.TextSerialTypeCode, Constants.OneByteNullSerialTypeCode)
    value = v
    isNull = isN
  }

  def getSerialCode = {
    isNull match {
      case true => nullSerialCode
      case false => valueSerialCode
    }
  }

  def getSize = {
    isNull match {
      case true => 0
      case false => value.length
    }
  }

}
