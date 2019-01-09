package io.model

case class BaseCondition(var index: Int, var condition: Short, var value: Object)

object BaseCondition {
  val Equals : Short = 0
  val LessThan : Short = 1
  val GreaterThan : Short = 2
  val LessThanEquals : Short = 3
  val GreaterThanEquals : Short = 4
}