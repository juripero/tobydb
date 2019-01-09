package query.model.result

class Result(var rowsChanged: Int = 0, var isInternal: Boolean = false) {

  def show: Unit = {
    isInternal match {
      case true => return
      case false =>
        rowsChanged == 1 match {
          case true => println("1 row affected")
          case false => println(s"$rowsChanged rows affected")
        }
    }
  }
}
