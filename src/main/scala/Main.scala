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
import akka.stream.scaladsl.Merge
import akka.stream.FlowShape
import NcaaPipeFilter.Question

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
  //
  val q1BalancerWorker = Flow[CsvRow]
    .mapConcat { elt =>
      if elt.day == "Sunday" then List(Answer(Question.SundayVictories, elt.winTeam, 1), Answer(Question.SundayVictories, elt.loseTeam, 0))
      else List(Answer(Question.SundayVictories, elt.winTeam, 0), Answer(Question.SundayVictories, elt.loseTeam, 0))
    }

  val q1Balancer = BalancerFlow(q1BalancerWorker)
  val q1limiter1 = Flow.fromGraph(new LimiterFlow[Answer](ans => ans.cntr == 0))
  val q1limiter2 = Flow.fromGraph(new LimiterFlow[Answer](ans => ans.cntr == 0))

  val broadcast = GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>
      import GraphDSL.Implicits._

      val broadcastShape = builder.add(Broadcast[CsvRow](1))
      val balancer = builder.add(q1Balancer)
      val limiter1 = builder.add(q1limiter1)
      val limiter2 = builder.add(q1limiter2)
      val logger = builder.add(Flow[Answer].map{elt => elt})
      val mergeShape = builder.add(Merge[Answer](1))

      broadcastShape ~> balancer ~> limiter1 ~> logger ~> mergeShape ~> limiter2

      FlowShape(broadcastShape.in, limiter2.out)
    }

  val anotherLimiter = Flow.fromGraph(new LimiterFlow[Answer](ans => ans.cntr == 0))

  val q1AggregatorFlow = Flow[Answer]
                        .groupBy(200, _.team)
                        .reduce {
                          (a, b) => Answer(a.qType, a.team, a.cntr + b.cntr)
                        } 
                        .mergeSubstreams

  val q1Sink = Sink.fromGraph(new WriterSink("Question1.txt", 20))

  val graph = Source.fromGraph(csvSource)
                .groupBy(1000, _.winTeam)
                .via(Flow.fromGraph(broadcast))
                .mergeSubstreams
                .via(anotherLimiter)
                .via(q1AggregatorFlow)
                .to(q1Sink)
                .run()

  
  // val q1BalancerWorker = Flow[CsvRow]
  //   .mapConcat { elt =>
  //     if elt.day == "Sunday" then List(SundayVictories(elt.winTeam, 1), SundayVictories(elt.loseTeam, 0))
  //     else List(SundayVictories(elt.winTeam, 0), SundayVictories(elt.loseTeam, 0))
  //   }
  //
  // val q1Balancer = BalancerFlow(q1BalancerWorker)
  //
  // val q1AggregatorFlow = Flow[SundayVictories]
  //                       .groupBy(1000, _.team)
  //                       .reduce {
  //                         (a, b) => SundayVictories(a.team, a.numberWins + b.numberWins)
  //                       } 
  //                       .mergeSubstreams
  //
  //

  // val graph = Source.fromGraph(csvSource)
  //               .via(q1Flow)
  //               .to(q1Sink)
  //               .run()

  /**********Q2*************/

  /*****Graph*********/

  // val graph = RunnableGraph.fromGraph( 
  //   GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>
  //     import GraphDSL.Implicits._
  //
  //     val broadcastShape = builder.add(Broadcast[CsvRow](4))
  //     
  //     val inputShape = builder.add(csvSource)
  //
  //     val q1FlowShape = builder.add(q1Flow)
  //     val q1SinkShape = builder.add(q1Sink)
  //
  //     val q2FlowShape = builder.add(q2Flow)
  //     val q2SinkShape = builder.add(q2Sink)
  //
  //     val q3FlowShape = builder.add(q3Flow)
  //     val q3SinkShape = builder.add(q3Sink)
  //
  //     val q4FlowShape = builder.add(q4Flow)
  //     val q4SinkShape = builder.add(q4Sink)
  //
  //                   broadcastShape ~> q2FlowShape ~> q2SinkShape
  //     inputShape ~> broadcastShape ~> q1FlowShape ~> q1SinkShape
  //                   broadcastShape ~> q3FlowShape ~> q3SinkShape
  //                   broadcastShape ~> q4FlowShape ~> q4SinkShape
  //
  //     ClosedShape
  //   }
  // ) 

  // graph.run()

   // graph.onComplete {
   //   case Success(_) => println("All elements have been processed")
   //   case Failure(exception) => println(s"An error occured with status ${exception}")
   // }

}
