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
 * Returns a BalancerFlow which splits incoming flow into two.
 * Each Balancer branch applies the `balancerWorker[I, O]` flow.
 * Results are then merged back.
 * Optionally a flow can be applied before the balancer.
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

  def apply[I, O](balancerWorker: Flow[I, O, Any], inFlow: Option[Flow[I, I, Any]] = None, outFlow: Option[Flow[O, O, Any]] = None) = 
    val customGraph = GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>
      import GraphDSL.Implicits._

      val balancerShape = builder.add(balancer(balancerWorker))

      (inFlow, outFlow) match {
        case (None, None) => FlowShape(balancerShape.in, balancerShape.out)
        case (Some(inflow), None) => {
          val inflowShape = builder.add(inflow)
          inflowShape ~> balancerShape
          FlowShape(inflowShape.in, balancerShape.out)
        }
        case (None, Some(outflow)) => {
          val outflowShape = builder.add(outflow)
          balancerShape ~> outflowShape
          FlowShape(balancerShape.in, outflowShape.out)
        }
        case (Some(inflow), Some(outflow)) => {
          val inflowShape = builder.add(inflow)
          val outflowShape = builder.add(outflow)
          inflowShape ~> balancerShape ~> outflowShape
          FlowShape(inflowShape.in, outflowShape.out)
        }
      }
    }
    Flow.fromGraph(customGraph)
}
