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
import akka.stream.scaladsl.Broadcast

//TODO: Impl backpressure

/**
 * Connects
 */
class CustomFlow[I, O]

object CustomFlow {

  def apply[I, O](balancers: Seq[Flow[I, O, Any]]) = 
    GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>
    import akka.stream.scaladsl.GraphDSL.Implicits.fanOut2flow

    val nbBalancers = balancers.length
    val broadcastShape = builder.add(Broadcast[I](nbBalancers))
    val mergeShape = builder.add(Merge[O](nbBalancers))

    balancers.foreach { balancer =>
      val balancerShape = builder.add(balancer)
      broadcastShape ~> balancerShape ~> mergeShape
    }
    
    FlowShape(broadcastShape.in, mergeShape.out)
 }


}
