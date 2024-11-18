package NcaaPipeFilter

import akka.stream.stage.GraphStage
import akka.stream.SourceShape
import akka.stream.Outlet
import akka.stream.Attributes
import akka.stream.stage.GraphStageLogic
import akka.stream.Materializer
import akka.NotUsed
import akka.stream.stage.OutHandler
import java.nio.file.Paths
import akka.stream.scaladsl.FileIO
import akka.stream.alpakka.csv.scaladsl.CsvParsing
import akka.stream.alpakka.csv.scaladsl.CsvToMap
import akka.stream.scaladsl.GraphDSL
import akka.stream.SourceShape
import akka.stream.scaladsl.Flow
import java.time.LocalDate
import akka.stream.Supervision
import akka.stream.ActorAttributes
import akka.event.Logging
import akka.actor.ActorSystem
import scala.util.Try
import scala.util.Success
import scala.util.Failure

class CsvReaderSource[T]

object CsvReaderSource {

  /**
  * @param path is the path to a csv file
  * @param transform is the function used to parse each row. Key of Map is the header of the column, Value of Map is entry in that column
  */
  def apply[T](path: String)(transform: Map[String, String] => T) = GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>
      import GraphDSL.Implicits._

      val resumeDecider = Supervision.resumingDecider

      val file = Paths.get(path)

      //Produces a mapping from lines of the CSV file to Map[String, String]
      val csvToMapReader = FileIO
        .fromPath(file)
        .via(CsvParsing.lineScanner())
        .via(CsvToMap.toMapAsStrings())

      //Converts Map[String, String] to object of type T
      val converterFlow = Flow[Map[String, String]]
        .map[T] { elt => 
          Try(transform(elt)) match {
            case Success(res) =>  res
            case Failure(exception) => 
              println(s"Could not parse element ${elt} because of ${exception}")
              throw exception //let supervision strategy handle the restart
          } 
        }
        .withAttributes(ActorAttributes.supervisionStrategy(resumeDecider))

      val source = builder.add(csvToMapReader)
      val transformflow = builder.add(converterFlow)

      source ~> transformflow

      SourceShape(transformflow.out)
    }
}

