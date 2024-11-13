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
/**
 * Splits incoming stream by `key`
 * Should implement backpressure/buffer
 */
class CustomFlow[I, O]

object CustomFlow {

  //from : https://doc.akka.io/libraries/akka-core/current/stream/stream-cookbook.html
  private def balancer[I, O](worker: Flow[I, O, Any]): Flow[I, O, NotUsed] = {
    import GraphDSL.Implicits._

    Flow.fromGraph(GraphDSL.create() { implicit builder =>

      val balancer = builder.add(Balance[I](2, waitForAllDownstreams = true))
      val merge = builder.add(Merge[O](2))

      for (_ <- 1 to 2) {
        balancer ~> worker.async ~> merge
      }

      FlowShape(balancer.in, merge.out)
    })
  }


  def apply[I, O, K](keyGen: I => K)(worker: Flow[I, O, Any])(aggregator: Flow[O, O, Any]) = 
    GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>

    //TODO: could probably do this directly on inlet or smtgh
    val groupByFlow = Flow[I].groupBy(1000, keyGen)

    val balancerFlow = balancer(worker)

    val fullFlow = groupByFlow
                  // .log("After Flow Split")
                  .via(balancerFlow)
                  .via(aggregator)
                  // .log("After Balancer")
                  // .withAttributes(Attributes
                  //   .logLevels(onElement = Logging.InfoLevel))
                  .mergeSubstreams

    val fullFlowShape = builder.add(fullFlow)

    FlowShape(fullFlowShape.in, fullFlowShape.out)
 }


}
