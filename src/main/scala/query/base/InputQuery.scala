package query.base

import query.model.result.Result

trait InputQuery {

  def executeQuery : Result
  def validateQuery : Boolean

}
