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
import akka.stream.SinkShape

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

  /*============================================= Solution 1 =============================================*/  

 /**********Q1*************/

  val q1BalancerWorker = Flow[CsvRow]
    .mapConcat { elt =>
      if elt.day == "Sunday" then List(Answer(Question.SundayVictories, elt.winTeam, 1), Answer(Question.SundayVictories, elt.loseTeam, 0))
      else List(Answer(Question.SundayVictories, elt.winTeam, 0), Answer(Question.SundayVictories, elt.loseTeam, 0))
   }

  val limiter1 = Flow.fromGraph(new LimiterFlow[Answer](ans => ans.cntr == 0))

  val q1Balancer = BalancerFlow(q1BalancerWorker, outFlow = Some(limiter1))

  val q1Flow = Flow[Answer]
                .filter(p => p.qType == Question.SundayVictories)
                .groupBy(200, _.team)
                .reduce {
                  (a, b) => Answer(a.qType, a.team, a.cntr + b.cntr)
                } 
                .mergeSubstreams

  val q1Sink = Sink.fromGraph(new WriterSink("Question1.txt", 20))
  
  /**********Q2*************/
  val q2BalancerWorker = Flow[CsvRow]
    .mapConcat { elt =>
      if elt.winPoints - elt.losePoints > 5 then List(Answer(Question.PointsVictories, elt.winTeam, 1), Answer(Question.PointsVictories, elt.loseTeam, 0))
      else List(Answer(Question.PointsVictories, elt.winTeam, 0), Answer(Question.PointsVictories, elt.loseTeam, 0))
    }

  val limiter2 = Flow.fromGraph(new LimiterFlow[Answer](ans => ans.cntr == 0))

  val q2Balancer = BalancerFlow(q2BalancerWorker, outFlow = Some(limiter2))

  val q2Flow = Flow[Answer]
                .filter(p => p.qType == Question.PointsVictories)
                .groupBy(200, _.team)
                .reduce {
                  (a, b) => Answer(a.qType, a.team, a.cntr + b.cntr)
                } 
                .mergeSubstreams

  val q2Sink = Sink.fromGraph(new WriterSink("Question2.txt", 20))

  /**********Q3*************/
  val q3BalancerWorker = Flow[CsvRow]
    .mapConcat { elt =>
      if (elt.round == 8) then List(Answer(Question.QuarterTimes, elt.winTeam, 1), Answer(Question.QuarterTimes, elt.loseTeam, 1))
      else List(Answer(Question.QuarterTimes, elt.winTeam, 0), Answer(Question.QuarterTimes, elt.loseTeam, 0))
    }

  //Adding a filter to only keep rounds of 64 and rounds of 8
  val q3preFilter = Flow[CsvRow]
    .filter {elt => (elt.round == 64 || elt.round == 8)}

  val limiter3 = Flow.fromGraph(new LimiterFlow[Answer](ans => ans.cntr == 0))

  val q3Balancer = BalancerFlow(q3BalancerWorker, inFlow = Some(q3preFilter), outFlow = Some(limiter3))

  val q3Flow = Flow[Answer]
                .filter(p => p.qType == Question.QuarterTimes)
                .groupBy(200, _.team)
                .reduce {
                  (a, b) => Answer(a.qType, a.team, a.cntr + b.cntr)
                } 
                .mergeSubstreams

  val q3Sink = Sink.fromGraph(new WriterSink("Question3.txt", 20))

  /*******Q4*********/
  val q4BalancerWorker = Flow[CsvRow]
    .mapConcat { elt =>
      if (elt.season >= 1980 && elt.season <= 1990) then List(Answer(Question.YearlyLosses, elt.winTeam, 0), Answer(Question.YearlyLosses, elt.loseTeam, 1))
      else List(Answer(Question.YearlyLosses, elt.winTeam, 0), Answer(Question.YearlyLosses, elt.loseTeam, 0))
    }

  val limiter4 = Flow.fromGraph(new LimiterFlow[Answer](ans => ans.cntr == 0))

  val q4Balancer = BalancerFlow(q4BalancerWorker, outFlow = Some(limiter4))

  val q4Flow = Flow[Answer]
                .filter(p => p.qType == Question.YearlyLosses)
                .groupBy(200, _.team)
                .reduce {
                  (a, b) => Answer(a.qType, a.team, a.cntr + b.cntr)
                } 
                .mergeSubstreams

  val q4Sink = Sink.fromGraph(new WriterSink("Question4.txt", 20))

  val substreamFlow = CustomFlow(Seq(q1Balancer, q2Balancer, q3Balancer, q4Balancer))

  val aggregateResult = GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>
      import GraphDSL.Implicits._

      val broadcastShape = builder.add(Broadcast[Answer](4))
      val q1flowShape = builder.add(q1Flow)
      val q1sinkShape = builder.add(q1Sink)

      val q2flowShape = builder.add(q2Flow)
      val q2sinkShape = builder.add(q2Sink)

      val q3flowShape = builder.add(q3Flow)
      val q3sinkShape = builder.add(q3Sink)

      val q4flowShape = builder.add(q4Flow)
      val q4sinkShape = builder.add(q4Sink)

      broadcastShape ~> q1flowShape ~> q1sinkShape
      broadcastShape ~> q2flowShape ~> q2sinkShape
      broadcastShape ~> q3flowShape ~> q3sinkShape
      broadcastShape ~> q4flowShape ~> q4sinkShape

      SinkShape(broadcastShape.in)
    }

  val graph = Source.fromGraph(csvSource)
                .groupBy(200, _.winTeam)
                .via(Flow.fromGraph(substreamFlow))
                .mergeSubstreams
                .to(Sink.fromGraph(aggregateResult))
                .run()

}
