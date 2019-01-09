package io.model

import common.Constants._

import scala.collection.mutable

case class Page[T](
   var pageType: Byte
   , var cellCount: Byte
   , var startAddress: Short
   , var rightNodeAddress: Int
   , var listOfRecordAddress: mutable.MutableList[Short]
   , var records: mutable.MutableList[T]
   , var number: Int)

object Page {

  def createEmptyPage[T] : Page[T] =
    Page[T](LeafTablePage, 0, 0, RightMostPage, mutable.MutableList(), mutable.MutableList[T](), 0)

  def createEmptyPage[T](num : Int) : Page[T] =
    Page[T](LeafTablePage, 0, 0, RightMostPage, mutable.MutableList(), mutable.MutableList[T](), num)

  def fixedHeaderLength = java.lang.Byte.BYTES * 2 + java.lang.Short.BYTES + java.lang.Integer.BYTES
}