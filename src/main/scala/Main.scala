package NcaaPipeFilter

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import NcaaPipeFilter.CsvReaderSource
import java.time.LocalDate
import akka.stream.scaladsl.Source
import NcaaPipeFilter.DoubleFlow
import akka.stream.scaladsl.Flow
import scala.util.Success
import scala.util.Failure
import akka.stream.scaladsl.Keep
import akka.stream.Attributes

object Main extends App {
  implicit val system : ActorSystem = ActorSystem("GraphBasics")
  implicit val materializer : ActorMaterializer = ActorMaterializer()
  import scala.concurrent.ExecutionContext.Implicits.global

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
  //Filtering before does not work as it will create empty streams for teams that didnt win on Sunday
  //Might be fixable if we can retrieve the key on which sublow has been split, so that we can init empty element to (name, 0)
  //
  // lazy val groupKey = (elt: CsvRow) => elt.winTeam 
  lazy val neutralElt = SundayVictories("", 0)

  val countFlow = Flow[CsvRow]
                    .fold(neutralElt) {(acc, rowElt) =>
                      if rowElt.day == "Sunday" then SundayVictories(rowElt.winTeam, acc.numberWins + 1)
                      else SundayVictories(rowElt.winTeam, acc.numberWins)
                    }
                    .filter(_.team != "") //TODO: for some reason produces empty values -> Check why


  val aggregatorFlow = Flow[SundayVictories]
                        .reduce {
                          (a, b) => SundayVictories(a.team, a.numberWins + b.numberWins)
                        }

  val q1Flow = CustomFlow[CsvRow, SundayVictories, String](_.winTeam)(countFlow)(aggregatorFlow)

  val q1Sink = Sink.fromGraph(new WriterSink("Question1.txt", 20))

  val graph = Source.fromGraph(csvSource)
                .via(q1Flow)
                .to(q1Sink)
                .run()

   // graph.onComplete {
   //   case Success(_) => println("All elements have been processed")
   //   case Failure(exception) => println(s"An error occured with status ${exception}")
   // }

}
