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
import akka.stream.OverflowStrategy
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import akka.Done
import akka.stream.ActorAttributes
import akka.stream.Supervision
import java.time.Instant

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
   * Each BalancerWorker procedes in the same way: it uses mapConcat with a relevant predicat for the current question been processed and produces a list of relevant elements to answer the question.
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
  implicit val limiterPredicate: Answer => Boolean = { answer =>
    answer.cntr match {
      case SimpleCount(0) => true //Pass at most 1 element when counter = 0
      case SimpleCount(_) => false //
      //Last case for pattern matching exhaustiveness, should never happen as no limiter is applied
      case _: WinLossCount => false 
    }
  }


  //General throttleFlow which will be added as entry to each BalancerFlow
  val throttleFlow = Flow[CsvRow].throttle(5, 1.second)

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
      val answerElt = Answer(Question.BigLoss, _, _);
      val neutralElt = answerElt(_, Count.simple(0));

      // NEW:
      // Changed Q1 here to check which team lost with more than 85 points
      if elt.losePoints > 85 then List(answerElt(elt.loseTeam, Count.simple(1)), neutralElt(elt.winTeam))
      else List(neutralElt(elt.loseTeam), neutralElt(elt.winTeam))
  }

  // LimiterFlow is from Q1 assignment. It lets each neutral element for each team through at most once.
  // Done in order to have all the teams represented in the final output, even those who do not 
  // fulfill the questions predicat without spamming neutral elemets.
  val limiter1 = Flow.fromGraph(new LimiterFlow[Answer])
  val balancer1 = BalancerFlow(balancerWorker1, inFlow=Some(throttleFlow), outFlow = Some(limiter1))

  val filterReduce1 = createFilterReduce{ ans => ans.qType == Question.BigLoss}

  val sink1 = CustomSink(filterReduce1, "Question1.txt")
  
  /**********Q2*************/
  val balancerWorker2 = createWorker { elt =>
      val answerElt = Answer(Question.PointsVictories, _, _);
      val neutralElt = answerElt(_, Count.simple(0));
      // Fixed "<" here: 5 points or fewer
      if elt.winPoints - elt.losePoints <= 5 then List(answerElt(elt.winTeam, Count.simple(1)), neutralElt(elt.loseTeam))
      else List(neutralElt(elt.winTeam), neutralElt(elt.loseTeam))
  }

  val limiter2 = Flow.fromGraph(new LimiterFlow[Answer])

  // NEW: added throttle
  val balancer2 = BalancerFlow(balancerWorker2, inFlow=Some(throttleFlow), outFlow = Some(limiter2))

  val filterReduce2 = createFilterReduce{ ans => ans.qType == Question.PointsVictories}

  val sink2 = CustomSink(filterReduce2, "Question2.txt")

  /**********Q3*************/
  // NEW: 
  // Each worker produces a new element containg the team that won with its number of points 
  // Still need an element for the loosing team because in case the loosing team happens to 
  // be a top5 wining team we still need its score for the average computation
  val balancerWorker3 = createWorker { elt =>
      List(Answer(Question.Top5, elt.winTeam, Count.winLoss(true, elt.winPoints))
          ,Answer(Question.Top5, elt.loseTeam, Count.winLoss(false, elt.losePoints)))
  }

  val balancer3 = BalancerFlow(balancerWorker3, inFlow = Some(throttleFlow))

  // Map reduce to produce 1 Element per team with info on won games, total played and score per game
  val filterReduce3 = createFilterReduce{ans => ans.qType == Question.Top5}

  def getWins(count: Count): Int = count match {
    case WinLossCount(wins, _, _) => wins
    case SimpleCount(_) => 0
  }

  // Produces a list from reduces values and sorts by number of won games to take top5
  val takeTop5 = Flow[Answer]
    .fold(List.empty[Answer])(_ :+ _)  // Need to collect all answers
    .map(answers => answers.sortBy(answer => -getWins(answer.cntr))
    .take(5)) //sort by highest wins and take 5
    .mapConcat(identity)  // Flattens to individual elems to be passed to print

  val q3SinkFlow = Flow[Answer]
    .via(filterReduce3)
    .via(takeTop5)

  val sink3 = CustomSink(q3SinkFlow, "Question3.txt")

  /*******Q4*********/
  val balancerWorker4 = createWorker { elt =>
      val answerElt = Answer(Question.YearlyLosses, _, _);
      val neutralElt = answerElt(_, Count.simple(0));
      if (elt.season >= 1980 && elt.season <= 1990) then List(answerElt(elt.winTeam, Count.simple(0)), answerElt(elt.loseTeam, Count.simple(1)))
      else List(neutralElt(elt.winTeam), neutralElt(elt.loseTeam))
  }

  val limiter4 = Flow.fromGraph(new LimiterFlow[Answer])
  val balancer4 = BalancerFlow(balancerWorker4, outFlow = Some(limiter4))

  val filterReduce4 = createFilterReduce{ ans => ans.qType == Question.YearlyLosses }

  val sink4 = CustomSink(filterReduce4, "Question4.txt")

  /******Building runnable Graph*******/
  val substreamFlow = CustomFlow(Seq(balancer1, balancer2, balancer3, balancer4))

  val dispatchAggregateResults = Sink.fromGraph (
    GraphDSL.create(sink1.async, sink2.async, sink3.async, sink4.async)((m1, m2, m3, m4) => Future.sequence(Seq(m1, m2, m3, m4))) { 
      implicit builder => (s1, s2, s3, s4) =>
      import GraphDSL.Implicits._

      val broadcastShape = builder.add(Broadcast[Answer](4))

      val sink1 : SinkShape[Answer] = s1.asInstanceOf[SinkShape[Answer]]
      val sink2 : SinkShape[Answer] = s2.asInstanceOf[SinkShape[Answer]]
      val sink3 : SinkShape[Answer] = s3.asInstanceOf[SinkShape[Answer]]
      val sink4 : SinkShape[Answer] = s4.asInstanceOf[SinkShape[Answer]]

      // broadcastShape ~> s1
      broadcastShape ~> sink1
      broadcastShape ~> sink2
      broadcastShape ~> sink3
      broadcastShape ~> sink4

      SinkShape(broadcastShape.in)
    }
  )
  
  val counter = AtomicLong(0)

  val countingFlow = Flow[Answer].map{elt => counter.incrementAndGet(); elt}

  val startTime = Instant.now()

  // limiter in worker: 9725
  // without limiter: 15078
  // limiter in worker + after worker: 8825

  val inputCounter = new AtomicLong(0)
  val outputCounter = new AtomicLong(0)

  val solution1Graph = Source.fromGraph(csvSource)
                .groupBy(200, _.winTeam)
                .async
                .map { elem => 
                  inputCounter.incrementAndGet()
                  elem
                }
                // Drop oldest element -- 5 elems per flow + buffer of 10 will lead to dropped values
                // If we change to backpressure and to throttle no elements are dropped
                .buffer(10, OverflowStrategy.dropHead) 
                .map { elem =>
                  val out = outputCounter.incrementAndGet()
                  val in = inputCounter.get()
                  if (in % 100 == 0) println(s"Input: $in, Output: $out, Dropped: ${in - out}")
                  elem
                }
                .via(substreamFlow)
                .mergeSubstreams
                .via(countingFlow)
                .toMat(dispatchAggregateResults)(Keep.right)
                .run()

}
