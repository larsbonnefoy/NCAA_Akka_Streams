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
import java.util.concurrent.atomic.AtomicLong

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
 /**
   * First solution provides in the final document an occuruence of every team, even teams for which the 
   * final response count is 0 (i.e. a team that has never won on a Sunday should still appear with a count of 0).
   *
   * Mostly based on MapReduce
   *
   * The Csv source is identical in both cases.
   *
   * Each BalancerWorker procedes in the same way: it uses mapConcat with a relevant predicat for the current question been processed and  
   * produces a list of relevant elements to answer the question.
   * It produces elements of type Answer(question, team, count) where question is used to identify to what type of question (Q1, 2, 3 or 4) this Answer object belongs. "team" is used as a key in the map reduce process and represents to what team the answer belongs. "count" is used as value and is equal to 1 if the predicat is satisfied, else count is equal to 0 and the produces element can be seen as the neutral element for that key,value pair.
   *
   * Each BalancerWorker produces a list of Answer elements, that hold either a 1 or a 0. That way, elements that do not verify the predicat are not filterted out and are sent down stream. 
   *
   * To avoid sending to many neutral elements down stream outputs of the balancerWorker are passed into a LimiterFlow which sents out at most one element for which the given predicate is true. If we check for the neutral element at that stage it ensure that only one neutral element is sent down stream
   *
   *
   */

 /*********/
 //Most of the questions are similar => define some helper functions

  implicit val limiterPredicate: Answer => Boolean = (ans: Answer) => (ans.cntr == 0) 

  /**
    * Creates a new worker which applies function f and attaches a limiter
    * @param f is the function carried out by the worker
    */
  def createWorker(f: CsvRow => IterableOnce[Answer]) = {
    val limiter = Flow.fromGraph(new LimiterFlow[Answer])
    Flow[CsvRow]
      .mapConcat(f)
      .via(limiter)
  }

  /**
    * Creates a new flow used for filtering and reducing
    * Keeps only values for which p == True. 
    * Splits incoming stream by team and reduces all incoming value, summming each individual counter.
    * This yields the final answer for each team
    *
    * @param p is the predicate used for selection
    * @return a flow that filters and reduces incoming values
    */
  def createFilterReduce(p: Answer => Boolean) = {
      Flow[Answer]
        .filter(p)
        .groupBy(200, _.team)
        .reduce {
          (a, b) => Answer(a.qType, a.team, a.cntr + b.cntr)
        } 
        .mergeSubstreams
  }


 /**********Q1*************/

  val balancerWorker1 = createWorker { elt => 
      val answerElt = Answer(Question.SundayVictories, _, _);
      val neutralElt = answerElt(_, 0);
      if elt.day == "Sunday" then List(answerElt(elt.winTeam, 1), neutralElt(elt.loseTeam))
      else List(neutralElt(elt.winTeam), neutralElt(elt.loseTeam))
  }

  val limiter1 = Flow.fromGraph(new LimiterFlow[Answer])
  val balancer1 = BalancerFlow(balancerWorker1, outFlow = Some(limiter1)) //could attach some limiter here

  val filterReduce1 = createFilterReduce{ ans => ans.qType == Question.SundayVictories}

  val sink1 = CustomSink(filterReduce1, "Question1.txt")
  
  /**********Q2*************/
  val balancerWorker2 = createWorker { elt =>
      val answerElt = Answer(Question.PointsVictories, _, _);
      val neutralElt = answerElt(_, 0);
      if elt.winPoints - elt.losePoints > 5 then List(answerElt(elt.winTeam, 1), neutralElt(elt.loseTeam))
      else List(neutralElt(elt.winTeam), neutralElt(elt.loseTeam))
  }

  val limiter2 = Flow.fromGraph(new LimiterFlow[Answer])
  val balancer2 = BalancerFlow(balancerWorker2, outFlow = Some(limiter2))

  val filterReduce2 = createFilterReduce{ ans => ans.qType == Question.PointsVictories}

  val sink2 = CustomSink(filterReduce2, "Question2.txt")

  /**********Q3*************/

  val balancerWorker3 = createWorker { elt =>
      val answerElt = Answer(Question.QuarterTimes, _, _);
      val neutralElt = answerElt(_, 0);
      if (elt.round == 8) then List(answerElt(elt.winTeam, 1), answerElt(elt.loseTeam, 1))
      else List(neutralElt(elt.winTeam), neutralElt(elt.loseTeam))
  }

  //Adding a filter to only keep rounds of 64 and rounds of 8
  val preFilter3 = Flow[CsvRow]
    .filter {elt => (elt.round == 64 || elt.round == 8)}

  val limiter3 = Flow.fromGraph(new LimiterFlow[Answer])
  val balancer3 = BalancerFlow(balancerWorker3, inFlow = Some(preFilter3), outFlow = Some(limiter3))

  val filterReduce3 = createFilterReduce{ ans => ans.qType == Question.QuarterTimes }

  val sink3 = CustomSink(filterReduce3, "Question3.txt")

  /*******Q4*********/
  val balancerWorker4 = createWorker { elt =>
      val answerElt = Answer(Question.YearlyLosses, _, _);
      val neutralElt = answerElt(_, 0);
      if (elt.season >= 1980 && elt.season <= 1990) then List(answerElt(elt.winTeam, 0), answerElt(elt.loseTeam, 1))
      else List(neutralElt(elt.winTeam), neutralElt(elt.loseTeam))
  }

  val limiter4 = Flow.fromGraph(new LimiterFlow[Answer])
  val balancer4 = BalancerFlow(balancerWorker4, outFlow = Some(limiter4))

  val filterReduce4 = createFilterReduce{ ans => ans.qType == Question.QuarterTimes }

  val sink4 = CustomSink(filterReduce4, "Question4.txt")

  /******Building runnable Graph*******/
  val substreamFlow = CustomFlow(Seq(balancer1, balancer2, balancer3, balancer4))

  val dispatchAggregateResults = Sink.fromGraph (
    GraphDSL.create() { 
      implicit builder: GraphDSL.Builder[NotUsed] =>
      import GraphDSL.Implicits._

      val broadcastShape = builder.add(Broadcast[Answer](4).async)

      val q1sinkShape = builder.add(sink1)
      val q2sinkShape = builder.add(sink2)
      val q3sinkShape = builder.add(sink3)
      val q4sinkShape = builder.add(sink4)

      broadcastShape ~> q1sinkShape
      broadcastShape ~> q2sinkShape
      broadcastShape ~> q3sinkShape
      broadcastShape ~> q4sinkShape

      SinkShape(broadcastShape.in)
    }
  )
  
  val counter = AtomicLong(0)

  val countingFlow = Flow[Answer].map{elt => counter.incrementAndGet(); elt}

  // limiter in worker: 9725
  // without limiter: 15078
  // limiter in worker + after worker: 8825

  val solution1Graph = Source.fromGraph(csvSource)
                .groupBy(200, _.winTeam)
                .async //each substreams runs in //
                // .map { elt =>
                //   println(s"I am running on thread [${Thread.currentThread().getId}]")
                //   elt
                // }
                .via(substreamFlow)
                .mergeSubstreams
                // .via(countingFlow)
                // .runWith(Sink.ignore)
                .to(dispatchAggregateResults)
                .run()

  // solution1Graph.onComplete { _ =>
  //     println(s"Total elements passed through: ${counter.get()}")
  // }

}
