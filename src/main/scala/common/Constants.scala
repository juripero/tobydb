package common

object Constants {

  val PageSize = 512
  val InteriorTablePage : Byte = 0x05
  val LeafTablePage : Byte = 0x0D.toByte
  val RightMostPage = 0xFFFFFFFF

  val Prompt = "toby>"
  val Version = "grrr"

  val DefaultFileExtension = ".tbl"
  val DataDirectoryName = "data"
  val CatalogDatabaseName = "catalog"
  val TobyTables = "toby_tables"
  val TobyColumns = "toby_columns"

  val InvalidClass: Byte = -1
  val TinyInt : Byte = 0
  val SmallInt : Byte = 1
  val Int : Byte = 2
  val BigInt : Byte = 3
  val Real : Byte = 4
  val Double : Byte = 5
  val Date : Byte = 6
  val DateTime : Byte = 7
  val Text : Byte = 8

  val OneByteNullSerialTypeCode = 0x00
  val TwoByteNullSerialTypeCode = 0x01
  val FourByteNullSerialTypeCode = 0x02
  val EightByteNullSerialTypeCode = 0x03
  val TinyIntSerialTypeCode = 0x04
  val SmallIntSerialTypeCode = 0x05
  val IntSerialTypeCode = 0x06
  val BigIntSerialTypeCode = 0x07
  val RealSerialTypeCode = 0x08
  val DoubleSerialTypeCode = 0x09
  val DateTimeSerialTypeCode = 0x0A
  val DateSerialTypeCode = 0x0B
  val TextSerialTypeCode = 0x0C

}
