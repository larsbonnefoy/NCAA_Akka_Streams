//TODO: Rename to main once other one is not needed anymore
package NcaaPipeFilter

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import NcaaPipeFilter.CsvReaderSource
import java.time.LocalDate
import akka.stream.scaladsl.Source
import NcaaPipeFilter.SundayVictoriesFlow

object Main extends App {
  implicit val system : ActorSystem = ActorSystem("GraphBasics")
  implicit val materializer : ActorMaterializer = ActorMaterializer()

  // val printMapSink = Sink.foreach[CsvRow](row => println(s"[${row.gameDate}] Won: ${row.winTeam}, Lost: ${row.loseTeam} on ${row.day}"))
  val printTotalRows = Sink.foreach[Int](println)

  val csvSource = CsvReaderSource("ressources/basketball.csv") {
    mapRow => CsvRow(mapRow("season"), 
                  mapRow("round"), 
                  mapRow("days_from_epoch"),
                  LocalDate.parse(mapRow("game_date")),
                  mapRow("day"), 
                  mapRow("win_alias"), 
                  mapRow("lose_alias"))
    }

  //TODO: Should convert days of the week to specific type to avoid checking on strings
  val sundayVictories = SundayVictoriesFlow[CsvRow, Int](0) {
    (count, csvRow) => if (csvRow.day == "Sunday") then count + 1 else count
  }

  //Source.fromGraph(csvSource).via(sundayVictories).runWith(printTotalRows)
  val source = Source.fromGraph(csvSource).groupBy(100, _.winTeam).to(Sink.fold(0)((count, row) => {
    println(s"Wins: ${row.winTeam} --> Won games on Sundays ${count}")
    if (row.day == "Sunday") then count + 1 else count
  })).run()

}
