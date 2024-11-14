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
import akka.stream.scaladsl.RunnableGraph
import akka.stream.scaladsl.GraphDSL
import akka.NotUsed
import akka.stream.scaladsl.Broadcast
import akka.stream.ClosedShape
import akka.event.Logging

object Main extends App {
  implicit val system : ActorSystem = ActorSystem("GraphBasics")
  implicit val materializer : ActorMaterializer = ActorMaterializer()
  import scala.concurrent.ExecutionContext.Implicits.global

  val UNIQUE_TEAMS = 186

  //TODO: What happens in case of error ? -> drop row i guess
  val csvSource = CsvReaderSource("ressources/basketball.csv") {
    fileRow => CsvRow(fileRow("season"), 
                  fileRow("round").toInt, 
                  fileRow("days_from_epoch"),
                  LocalDate.parse(fileRow("game_date")),
                  fileRow("day"), 
                  fileRow("win_name"), 
                  fileRow("lose_name"), 
                  fileRow("win_pts").toInt, 
                  fileRow("lose_pts").toInt)
    }

  /**********Q1*************/

  //TODO: Should convert days of the week to specific type to avoid checking on strings
  //Filtering before does not work as it will create empty streams for teams that didnt win on Sunday
  //Might be fixable if we can retrieve the key on which sublow has been split, so that we can init empty element to (name, 0)

  lazy val neutralElt = SundayVictories("", 0)

  val q1CountFlow = Flow[CsvRow]
                    // .map(e => e.winTeam -> (if e.day == "Sunday" then 1 else 0))
                    .fold(neutralElt) {(acc, rowElt) =>
                      if rowElt.day == "Sunday" then SundayVictories(rowElt.winTeam, acc.numberWins + 1)
                      else SundayVictories(rowElt.winTeam, acc.numberWins)
                    }
                    .filter(_.team != "") //TODO: for some reason produces empty values -> Check why


  val q1AggregatorFlow = Flow[SundayVictories]
                        .reduce {
                          (a, b) => SundayVictories(a.team, a.numberWins + b.numberWins)
                        }

  val q1Flow = BalancerFlow[CsvRow, SundayVictories, String](_.winTeam)(q1CountFlow)(q1AggregatorFlow)

  val q1Sink = Sink.fromGraph(new WriterSink("Question1.txt", 20))

  /**********Q2*************/

  //TODO: q1 and q2 are really similar, could not be merge ? as the key is the same
  //!!! this strategy implies that each team has at least won once so that it is displayed !!
  val q2CountFlow = Flow[CsvRow]
                    .fold(PointsVictories("", 0)) {(acc, rowElt) =>
                      if rowElt.winPoints - rowElt.losePoints > 5 then PointsVictories(rowElt.winTeam, acc.numberWins + 1)
                      else PointsVictories(rowElt.winTeam, acc.numberWins)
                    }
                    .filter(_.team != "") //TODO: for some reason produces empty values -> Check why

  val q2AggregatorFlow = Flow[PointsVictories]
                        .reduce {
                          (a, b) => PointsVictories(a.team, a.numberWins + b.numberWins)
                        }

  val q2Flow = BalancerFlow[CsvRow, PointsVictories, String](_.winTeam)(q2CountFlow)(q2AggregatorFlow)

  val q2Sink = Sink.fromGraph(new WriterSink("Question2.txt", 20))

  /*******Q3***********/

  val q3SubstreamWorker = Flow[CsvRow]
    .mapConcat { elt => 
      if (elt.round == 64) {
        List(RoundInstances(elt.winTeam, 0), RoundInstances(elt.loseTeam, 0))
      }
      else {
        List(RoundInstances(elt.winTeam, 1), RoundInstances(elt.loseTeam, 1))
      }
    }
    .groupBy(1000, _.team)
    .reduce((l, r) => RoundInstances(l.team, l.number + r.number))
    .mergeSubstreams
  
  val q3AggregatorFlow = Flow[RoundInstances]
    .map {elt => println("Before q3 Aggreg" + elt) ; elt}
    .groupBy(1000, _.team)
    .reduce((l, r) => RoundInstances(l.team, l.number + r.number))
    .mergeSubstreams
    .map {elt => println("After q3 Aggreg" + elt) ; elt}

  //Apply filter first to have only rows we care about
  val q3PreFilter = (rowElt: CsvRow) => (rowElt.round == 64 || rowElt.round == 8)
  val q3Flow = BalancerFlow[CsvRow, RoundInstances, Int](_.round)(q3SubstreamWorker)(q3AggregatorFlow)

  val q3Sink = Sink.fromGraph(new WriterSink("Question3.txt", 20))

  val graph = Source.fromGraph(csvSource)
                .filter(q3PreFilter)
                .via(q3Flow)
                // .via(lastReduce)
                // .to(Sink.ignore)
                // .toMat(Sink.seq)(Keep.right)
                .to(q3Sink)
                .run()

  val q1q2G = RunnableGraph.fromGraph( 
    GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>
      import GraphDSL.Implicits._

      val splitterShape = builder.add(Broadcast[CsvRow](2))
      
      val inputShape = builder.add(csvSource)

      val q1FlowShape = builder.add(q1Flow)
      val q1SinkShape = builder.add(q1Sink)

      val q2FlowShape = builder.add(q2Flow)
      val q2SinkShape = builder.add(q2Sink)

      inputShape ~> splitterShape ~> q1FlowShape ~> q1SinkShape
                    splitterShape ~> q2FlowShape ~> q2SinkShape

      ClosedShape
    }
  ) 

  //q1q2G.run()

   // graph.onComplete {
   //   case Success(_) => println("All elements have been processed")
   //   case Failure(exception) => println(s"An error occured with status ${exception}")
   // }

}
