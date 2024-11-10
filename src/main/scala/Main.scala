import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.stream.scaladsl.RunnableGraph
import akka.stream.scaladsl.GraphDSL
import akka.NotUsed
import akka.stream.scaladsl.Broadcast
import akka.stream.scaladsl.Sink
import akka.stream.ClosedShape
import akka.stream.scaladsl.Merge
import akka.stream.scaladsl.Balance
import akka.stream.scaladsl.Flow
import scala.concurrent.Future
import akka.stream.FlowShape
import akka.stream.scaladsl.Zip
import akka.stream.scaladsl.Unzip
import akka.stream.scaladsl.MergePreferred
import scala.concurrent.duration._
import akka.stream.OverflowStrategy

object GraphBasics extends App {
  println("Hello from Graph basics")

  implicit val system : ActorSystem = ActorSystem("GraphBasics")
  implicit val materializer : ActorMaterializer = ActorMaterializer()

  val input = Source(1 to 1000)
  val sink1 = Sink.foreach[Int](println)
  val sink2 = Sink.foreach[Int](println)

  val g1 = RunnableGraph.fromGraph( 
    GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>
      import GraphDSL.Implicits._

      val splitter = builder.add(Broadcast[Int](2))
      // input ~> splitter 
      // splitter.out(0) ~> sink1
      // splitter.out(1) ~> sink2

      //Use implicit port forwarding to have a shorter notation

      input ~>  splitter ~> sink1
                splitter ~> sink2

      ClosedShape
    }
  )
  // g1.run()

  import scala.concurrent.duration._
  val fastSource = input.throttle(5, 1.second)
  val slowSource = input.throttle(2, 1.second)

  val sink3 = Sink.fold[Int, Int](1)((count, _) => {
    println(s"sink3: values $count")
    count + 1
  })
  val sink4 = Sink.fold[Int, Int](1)((count, _) => {
    println(s"sink4: values $count")
    count + 1
  })

  val g2 = RunnableGraph.fromGraph( 
    GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>
      import GraphDSL.Implicits._

      val merger = builder.add(Merge[Int](2))
      val balancer = builder.add(Balance[Int](2))

      //Use implicit port forwarding to have a shorter notation
      fastSource ~> merger ~> balancer ~> sink3
      slowSource ~> merger
      balancer ~> sink4 //merger already feeds into balancer => should not add it twice

      ClosedShape
    }
  )
  // g2.run()
  //
  //

  // def enhanceFlow[A, B](flow: Flow[A, B, ?]): Flow[A, B, Future[Int]] = {
  //   val counterSink = Sink.fold[Int, B](0)((count, _) => count + 1)
  //   Flow.fromGraph(
  //     GraphDSL.create(counterSink) {implicit builder => counterSinkShape =>
  //       import GraphDSL.Implicits._
  //
  //       val broadcast = builder.add(Broadcast[B](2))
  //       val originalFlowShape = builder.add(flow)
  //
  //       originalFlowShape ~> broadcast
  //       broadcast ~> counterSinkShape //smthg wrong here idk what
  //
  //       FlowShape(originalFlowShape.in, broadcast.out(1))
  //
  //     }
  //
  //     )
  // }
  //
  // val simpleSource = Source(1 to 42)
  // val simpleFlow = Flow[Int].map(x => x)
  // val simpleSink = Sink.ignore
  //
  // simpleSource.via(enhanceFlow(simpleFlow)).to(simpleSink)
  //
  def fibonaciGraph = RunnableGraph.fromGraph(
    GraphDSL.create() {
    implicit builder =>
    import GraphDSL.Implicits._

    val source1 = Source[Int](Seq(1))
    val source2 = Source[Int](Seq(1))

    val fstArg = builder.add(MergePreferred[Int](1))
    val sndArg = builder.add(MergePreferred[Int](1))

    val combiner = builder.add(Zip[Int, Int]())

    //flow takes in s - 1, s - 2 and returns s and and s - 1
    // val fibFlow = builder.add(
    //   Flow[(Int, Int)]
    //     .map { case (s1 , s2) => (s1 + s2, s1)}
    // )

    val fibFlowDelay = builder.add(
      Flow[(Int, Int)]
        .map { pair => 
          Thread.sleep(10)
          val last = pair._1
          val prev = pair._2
          (last + prev, last )
        }
    )

    //out0 = S, out1 = S - 1
    //out1 goes into broadcast 
    //S - 1 goes into sndArg
    val splitter = builder.add(Unzip[Int, Int]())

    //Broadcast S to sink and back into fstArg
    val broadcast = builder.add(Broadcast[Int](2))

    val printSink = builder.add(Sink.foreach[Int](println))
    // val ignoreSink = builder.add(Sink.ignore)

    source1 ~> fstArg ~> combiner.in0
    source2 ~> sndArg ~> combiner.in1

    combiner.out ~> fibFlowDelay ~> splitter.in
    splitter.out0 ~> broadcast 
    // splitter.out1 ~> ignoreSink

    broadcast ~> printSink

    fstArg.preferred <~ broadcast
    sndArg.preferred <~ splitter.out1

    ClosedShape
  })

  fibonaciGraph.run()

}
