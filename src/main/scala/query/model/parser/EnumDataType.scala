package query.model.parser

class EnumDataType extends Enumeration {
  val TinyIntNull, SmallIntNull, IntNull, DoubleDateTimeNull, TinyInt, SmallInt, Int, BigInt, Real, Double, DateTime,
  Date, Text = Value
}

object EnumDataType extends Enumeration {
  type EnumDataType = Value
  val TinyIntNull, SmallIntNull, IntNull, DoubleDateTimeNull, TinyInt, SmallInt, Int, BigInt, Real, Double, DateTime,
  Date, Text = Value
}
