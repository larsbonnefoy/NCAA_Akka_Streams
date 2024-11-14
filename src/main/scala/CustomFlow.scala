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

//TODO: Impl backpressure

/**
 *
 * Splits incoming stream of type `I` with `keyGen` in n substreams and feeds each substream into a Balancer.
 * All resulting streams are then merged back together.
 * After that each stream has been split and merged back together, it might be that further processing is required.
 * An aggregator can be provided to further process the data
 */
class CustomFlow[I, O]

object CustomFlow {

  def apply[I, O, K](keyGen: I => K)(balancer: Flow[I, O, NotUsed])(aggregator: Flow[O, O, Any]) = 
    GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>

    //TODO: could probably do this directly on inlet or smtgh
    val groupByFlow = Flow[I].groupBy(1000, keyGen)

    val fullFlow = groupByFlow
                  // .log("After Flow Split")
                  .via(balancer)
                  .mergeSubstreams
                  .via(aggregator)
                  // .log("After Flow Balancer")
                  // .withAttributes(Attributes
                  //   .logLevels(onElement = Logging.InfoLevel))

    val fullFlowShape = builder.add(fullFlow)

    FlowShape(fullFlowShape.in, fullFlowShape.out)
 }


}
