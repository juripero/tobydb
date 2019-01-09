package query.model.parser

import query.QueryHandler

case class Condition(column: String, operator: Operator.Value, literal: Literal) {
  def fetchColumn = column
  def fetchLiteral = literal
}

object Condition {

  def getOperator(cd: String) = {

    (cd.contains("<="), cd.contains("<"), cd.contains(">="), cd.contains(">"), cd.contains("=")) match {
      case (true, _, _, _, _) => Operator.LessThanEqual
      case (false, true, _, _, _) => Operator.LessThan
      case (_, _, true, _, _) => Operator.GreaterThanEqual
      case (_, _, false, true, _) => Operator.GreaterThan
      case (false, false, false, false, true) => Operator.Equals
      case (_, _, _, _, _) => null
    }
  }

  def getInternalCondition(cd: String, op: Operator.Value, opString: String) : Condition = {
    val div = cd.split(opString)
    div.length < 2 match {
      case true =>
        QueryHandler.incorrectCommand(cd, "Invalid Condition Format")
        null
      case false =>
        val column = div(0).trim
        val literal = Literal.createLiteral(div(1).trim)
        Condition(column, op, literal)
    }
  }

  def createCondition(condition: String): Condition = {
    val op = Condition.getOperator(condition)
    if(op == null){
      QueryHandler.incorrectCommand(condition, "Invalid Operator")
      return null
    }

    op match {
      case Operator.GreaterThan => Condition.getInternalCondition(condition, op, ">")
      case Operator.LessThan => Condition.getInternalCondition(condition, op, "<")
      case Operator.LessThanEqual => Condition.getInternalCondition(condition, op, "<=")
      case Operator.GreaterThanEqual => Condition.getInternalCondition(condition, op, ">=")
      case Operator.Equals => Condition.getInternalCondition(condition, op, "=")
    }
  }
}
