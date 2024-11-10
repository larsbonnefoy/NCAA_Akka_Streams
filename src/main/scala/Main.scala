//TODO: Rename to main once other one is not needed anymore
package NcaaPipeFilter

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import NcaaPipeFilter.CsvReaderSource
import java.time.LocalDate
import akka.stream.scaladsl.Source

object RunApp extends App {
  implicit val system : ActorSystem = ActorSystem("GraphBasics")
  implicit val materializer : ActorMaterializer = ActorMaterializer()

  val printMapSink = Sink.foreach[CsvRow](row => println(s"[${row.gameDate}] Won: ${row.winTeam}, Lost: ${row.loseTeam} on ${row.day}"))

  val csvSource = CsvReaderSource("ressources/basketball.csv") {
    mapRow => CsvRow(mapRow("season"), 
                  mapRow("round"), 
                  mapRow("days_from_epoch"),
                  LocalDate.parse(mapRow("game_date")),
                  mapRow("day"), 
                  mapRow("win_alias"), 
                  mapRow("lose_alias"))
    }

  Source.fromGraph(csvSource).runWith(printMapSink)

}
