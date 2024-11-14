package NcaaPipeFilter

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import NcaaPipeFilter.CsvReaderSource
import java.time.LocalDate
import akka.stream.scaladsl.Source
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

  val UNIQUE_TEAMS = 187

  //TODO: What happens in case of error ? -> drop row i guess
  val csvSource = CsvReaderSource("ressources/basketball.csv") {
    fileRow => CsvRow(fileRow("season").toInt, 
                  fileRow("round").toInt, 
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

  val q1BalancerWorker = Flow[CsvRow]
    .mapConcat { elt =>
      if elt.day == "Sunday" then List(SundayVictories(elt.winTeam, 1), SundayVictories(elt.loseTeam, 0))
      else List(SundayVictories(elt.winTeam, 0), SundayVictories(elt.loseTeam, 0))
    }

  val q1Balancer = BalancerFlow(q1BalancerWorker)

  val q1AggregatorFlow = Flow[SundayVictories]
                        .groupBy(1000, _.team)
                        .reduce {
                          (a, b) => SundayVictories(a.team, a.numberWins + b.numberWins)
                        } 
                        .mergeSubstreams

  val q1Flow = CustomFlow[CsvRow, SundayVictories, String](_.winTeam)(q1Balancer)(q1AggregatorFlow)

  val q1Sink = Sink.fromGraph(new WriterSink("Question1.txt", 20))

  // val graph = Source.fromGraph(csvSource)
  //               .via(q1Flow)
  //               .to(q1Sink)
  //               .run()

  /**********Q2*************/

  //TODO: q1 and q2 are really similar, only difference is the predicate and type of the solution
  val q2BalancerWorker = Flow[CsvRow]
    .mapConcat { elt =>
      if elt.winPoints - elt.losePoints > 5 then List(PointsVictories(elt.winTeam, 1), PointsVictories(elt.loseTeam, 0))
      else List(PointsVictories(elt.winTeam, 0), PointsVictories(elt.loseTeam, 0))
    }

  val q2Balancer = BalancerFlow(q2BalancerWorker)

  val q2AggregatorFlow = Flow[PointsVictories]
                        .groupBy(1000, _.team)
                        .reduce {
                          (a, b) => PointsVictories(a.team, a.numberWins + b.numberWins)
                        } 
                        .mergeSubstreams

  val q2Flow = CustomFlow[CsvRow, PointsVictories, String](_.winTeam)(q2Balancer)(q2AggregatorFlow)

  val q2Sink = Sink.fromGraph(new WriterSink("Question2.txt", 20))

  /*******Q3***********/

  val q3BalancerWorker = Flow[CsvRow]
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

  val q3balancer = BalancerFlow(q3BalancerWorker)
  
  val q3AggregatorFlow = Flow[RoundInstances]
    .groupBy(1000, _.team)
    .reduce((l, r) => RoundInstances(l.team, l.number + r.number))
    .mergeSubstreams

  val q3PreFilter = (rowElt: CsvRow) => (rowElt.round == 64 || rowElt.round == 8)

  val q3Flow = CustomFlow[CsvRow, RoundInstances, Int](_.round)(q3balancer)(q3AggregatorFlow)

  val q3Sink = Sink.fromGraph(new WriterSink("Question3.txt", 20))

  /*******Q4***********/
  //Because games are only from march, years == Season
  val q4BalancerWorker = Flow[CsvRow]
    .mapConcat { elt =>
      if (elt.season >= 1980) && (elt.season <= 1990) then List(YearlyLosses(elt.winTeam, 0), YearlyLosses(elt.loseTeam, 1))
      else List(YearlyLosses(elt.winTeam, 0), YearlyLosses(elt.loseTeam, 0))
    }

  val q4Balancer = BalancerFlow(q4BalancerWorker)

  val q4AggregatorFlow = Flow[YearlyLosses]
                        .groupBy(1000, _.team)
                        .reduce {
                          (a, b) => YearlyLosses(a.team, a.numberLosses + b.numberLosses)
                        } 
                        .mergeSubstreams

  val q4Flow = CustomFlow[CsvRow, YearlyLosses, String](_.winTeam)(q4Balancer)(q4AggregatorFlow)

  val q4Sink = Sink.fromGraph(new WriterSink("Question4.txt", 20))

  /*****Graph*********/

  val graph = RunnableGraph.fromGraph( 
    GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>
      import GraphDSL.Implicits._

      val broadcastShape = builder.add(Broadcast[CsvRow](4))
      
      val inputShape = builder.add(csvSource)

      val q1FlowShape = builder.add(q1Flow)
      val q1SinkShape = builder.add(q1Sink)

      val q2FlowShape = builder.add(q2Flow)
      val q2SinkShape = builder.add(q2Sink)

      val q3FlowShape = builder.add(q3Flow)
      val q3SinkShape = builder.add(q3Sink)

      val q4FlowShape = builder.add(q4Flow)
      val q4SinkShape = builder.add(q4Sink)

                    broadcastShape ~> q2FlowShape ~> q2SinkShape
      inputShape ~> broadcastShape ~> q1FlowShape ~> q1SinkShape
                    broadcastShape ~> q3FlowShape ~> q3SinkShape
                    broadcastShape ~> q4FlowShape ~> q4SinkShape

      ClosedShape
    }
  ) 

  graph.run()

   // graph.onComplete {
   //   case Success(_) => println("All elements have been processed")
   //   case Failure(exception) => println(s"An error occured with status ${exception}")
   // }

}
