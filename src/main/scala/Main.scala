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

  // val config = com.typesafe.config.ConfigFactory.load()
  // println(config.getString("some.config.value"))
  

  // val printMapSink = Sink.foreach[CsvRow](row => println(s"[${row.gameDate}] Won: ${row.winTeam}, Lost: ${row.loseTeam} on ${row.day}"))
  val printTotalRows = Sink.foreach[Int](println)
  val printSundayVictories = Sink.foreach[SundayVictories](vic => println(s"Wins: ${vic.team} --> Won games on Sundays ${vic.numberWins}"))

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
                    .fold(SundayVictories("", 0)) {(acc, rowElt) => //println(acc);
                    (SundayVictories(rowElt.winTeam, acc.numberWins + 1))}

  val q1Flow = CustomFlow[CsvRow, SundayVictories, String](_.winTeam)(countFlow)

  val source = Source.fromGraph(csvSource)
                .via(q1Flow)
                .addAttributes(
                      Attributes.logLevels(
                        onElement = Attributes.LogLevels.Info,
                        onFinish = Attributes.LogLevels.Info,
                        onFailure = Attributes.LogLevels.Error))
                .log(name = "myStream")
                .toMat(Sink.foreach(_ => println("??????")))(Keep.right)

  // source.run()

  // source.onComplete {
  //   case Success(_) => println("All elements have been processed")
  //   case Failure(exception) => println(s"An error occured with status ${exception}")
  // }

  // source.onComplete {
  //   case whatever => println(s"${whatever}")
  // }

  val g1 = Source(1 to 10000).runWith(Sink.foreach(println))

  g1.onComplete {
    case whatever => println(whatever)
  }

  // val source = Source.fromGraph(csvSource).groupBy(100, _.winTeam).via(sundayVictories).to(printSundayVictories)
  // source.run()
}
