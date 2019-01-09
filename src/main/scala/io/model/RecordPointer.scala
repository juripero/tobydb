package io.model

case class RecordPointer(var leftPage: Int, var key: Int, var page: Int, var offset: Short)

object RecordPointer {
  def size = java.lang.Integer.BYTES * 2
}