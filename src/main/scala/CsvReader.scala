package NcaaPipeFilter

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import java.nio.file.Paths
import akka.stream.scaladsl.FileIO
import akka.stream.scaladsl.Framing
import akka.stream.alpakka.csv.scaladsl.CsvParsing
import akka.stream.alpakka.csv.scaladsl.CsvToMap
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.RunnableGraph
import akka.stream.scaladsl.GraphDSL
import akka.NotUsed
import akka.stream.ClosedShape
import akka.stream.scaladsl.Flow
import NcaaPipeFilter.CsvRow
import java.time.LocalDate

/**
  * from :
  * - https://stackoverflow.com/questions/40224457/reading-a-csv-files-using-akka-streams
  * - https://www.baeldung.com/scala/akka-streams-read-csv-file
  */
object CSVReader extends App {
  implicit val system : ActorSystem = ActorSystem("GraphBasics")
  implicit val materializer : ActorMaterializer = ActorMaterializer()

  val file = Paths.get("ressources/basketball.csv")

  val printMapSink = Sink.foreach[CsvRow](row => println(s"[${row.gameDate}] Won: ${row.winTeam}, Lost: ${row.loseTeam} on ${row.day}"))

  val converterFlow = Flow[Map[String, String]].map[CsvRow]{
    mapRow => CsvRow(mapRow("season"), 
              mapRow("round"), 
              mapRow("days_from_epoch"),
              LocalDate.parse(mapRow("game_date")),
              mapRow("day"), 
              mapRow("win_alias"), 
              mapRow("lose_alias"))
  }

  val csvToMapReader = FileIO
                      .fromPath(file)
                      .via(CsvParsing.lineScanner())
                      .via(CsvToMap.toMapAsStrings())

  val g = RunnableGraph.fromGraph( 
    GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>
      import GraphDSL.Implicits._

      val csvSourceShape = builder.add(csvToMapReader)
      val convert = builder.add(converterFlow)
      val printSinkShape = builder.add(printMapSink)
      csvSourceShape ~> convert ~> printSinkShape

      ClosedShape
    }
  )
  g.run()
}
