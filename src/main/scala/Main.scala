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
  val countFlow = Flow[CsvRow]
                    .filter(_.day == "Sunday")
                    .fold(SundayVictories("", 0)) {(acc, rowElt) =>
                    (SundayVictories(rowElt.winTeam, acc.numberWins + 1))}
                    .filter(_.team != "") //TODO: for some reason produces empty values

  val aggregatorFlow = Flow[SundayVictories]
                        .map {elt => println("???: " + elt); elt}
                        .reduce((a, b) => SundayVictories(a.team, a.numberWins + b.numberWins))

  val q1Flow = CustomFlow[CsvRow, SundayVictories, String](_.winTeam)(countFlow)(aggregatorFlow)

  val source = Source.fromGraph(csvSource)
                .via(q1Flow)
                .toMat(Sink.foreach(println))(Keep.right)
                .run()

   source.onComplete {
     case Success(_) => println("All elements have been processed")
     case Failure(exception) => println(s"An error occured with status ${exception}")
   }

}
