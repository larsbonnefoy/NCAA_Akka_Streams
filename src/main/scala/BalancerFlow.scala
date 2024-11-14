package NcaaPipeFilter

import akka.stream.scaladsl.GraphDSL
import akka.NotUsed
import akka.stream.scaladsl.Flow
import akka.stream.FlowShape
import akka.stream.scaladsl.Balance
import akka.stream.scaladsl.Merge
import akka.stream.Inlet
import akka.stream.Attributes
import akka.event.Logging
import akka.stream.Graph

//TODO: Impl backpressure

/**
 *
 * Splits incoming stream `I` with `keyGen` in n substreams and feeds each substream into a Balancer.
 * Each Balancer branch applies the `balancerWorker[I, O]` flow and is then merge back.
 * This results in one substream per key of type `O`.
 * All resulting streams are then merged back together.
 * This means that results must be merged back further via the 
 */
class BalancerFlow[I, O, M]

object BalancerFlow {

  //from : https://doc.akka.io/libraries/akka-core/current/stream/stream-cookbook.html
  private def balancer[I, O](balancerWorker: Flow[I, O, Any]): Flow[I, O, NotUsed] = {
    import GraphDSL.Implicits._

    Flow.fromGraph(GraphDSL.create() { implicit builder =>

      val balancer = builder.add(Balance[I](2, waitForAllDownstreams = true))
      val merge = builder.add(Merge[O](2))

      for (i <- 1 to 2) {
        val workerWithLogs = balancerWorker
                                .log(s"worker${i}")
                                .withAttributes(Attributes
                                  .logLevels(onElement = Logging.InfoLevel))

        balancer ~> balancerWorker.async ~> merge
        //balancer ~> workerWithLogs.async ~> merge
      }

      FlowShape(balancer.in, merge.out)
    })
  }

  def apply[I, O](balancerWorker: Flow[I, O, Any]) = 
    val customGraph = GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>
      val fullFlowShape = builder.add(balancer(balancerWorker))
      FlowShape(fullFlowShape.in, fullFlowShape.out)
    }
    Flow.fromGraph(customGraph)


}
