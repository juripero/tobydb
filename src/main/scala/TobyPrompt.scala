import java.util.Scanner

import common.{CatalogHelper, Constants}
import query.QueryHandler
import query.QueryParser._

object TobyPrompt {

  val sc = new Scanner(System.in).useDelimiter(";")
  CatalogHelper.initialiseDatabase
  displayOnScreen
  while(!stop){
    println(Constants.Prompt)
    val cmd = sc.next().replace("\n", "").replace("\r", " ").trim.toLowerCase
    parseUserCommand(cmd.toLowerCase)
  }

  def displayOnScreen = {
    println(QueryHandler.line("-", 90))
    println("TobyDB is on")
    QueryHandler.getVersion
    println("Use help to display command list")
    println(QueryHandler.line("-", 90))
  }
}
