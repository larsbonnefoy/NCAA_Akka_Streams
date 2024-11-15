package NcaaPipeFilter

import akka.stream.stage.GraphStage
import akka.stream.SinkShape
import akka.stream.Inlet
import akka.stream.Attributes
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.InHandler
import scala.collection.mutable
import java.io.PrintWriter
import java.io.File
import java.io.IOException
import scala.util.Try
import scala.util.Using
import java.io.FileWriter
import akka.stream.Outlet
import akka.stream.FlowShape
import akka.stream.stage.OutHandler


//back pressure will work automatically
/**
  * Flow that sends only one value out for which predicate == True
  */
class  LimiterFlow[T](predicate: T => Boolean) extends GraphStage[FlowShape[T, T]] {

  val inPort = Inlet[T]("LimiterIn")
  val outPort = Outlet[T]("LimiterOut")

  override def shape: FlowShape[T, T] = FlowShape(inPort, outPort)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    val memo = new mutable.HashSet[T]

    //
    setHandler(outPort, new OutHandler {
      override def onPull(): Unit = pull(inPort)
    })

    setHandler(inPort, new InHandler {

      override def onPush(): Unit = {
        val nextElement = grab(inPort)
        
        if (predicate(nextElement)) {
          if (!memo.contains(nextElement)) {
            // println("Passing: " + nextElement)
            push(outPort, nextElement)
            memo += nextElement
          } else {
            pull(inPort) //do not push elt and ask for another elt
          }
        } 
        else {
          // println("Passing: " + nextElement)
          push(outPort, nextElement)
        }
      }

    })
  }
}
