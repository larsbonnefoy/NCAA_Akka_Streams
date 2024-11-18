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
import akka.stream.stage.GraphStageWithMaterializedValue
import scala.concurrent.Future
import akka.Done
import scala.concurrent.Promise


//back pressure will work automatically
class  BatchWriter[T](path: String, batchSize: Int = 20) extends GraphStageWithMaterializedValue[SinkShape[T], Future[Done]] {

  val inPort = Inlet[T]("writer")

  override def shape: SinkShape[T] = SinkShape[T](inPort)

  def writeToFile(filePath: String, str: String): Try[Unit] = {
    Using(new FileWriter(path, true)) { writer =>
      writer.write(str + "\n")
      writer.close()
    }
  }

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
    val promise = Promise[Done]

    val logic = new GraphStageLogic(shape) {

      val batch = new mutable.Queue[T]
      val writer = new PrintWriter(new File(path))
      
      //Need to start demand process first as upstream elements are waiting for "onPull"
      override def preStart(): Unit = {
        pull(inPort)
      }

      setHandler(inPort, new InHandler {

        override def onPush(): Unit = {
          val nextElement = grab(inPort)
          batch.enqueue(nextElement)
          if (batch.size >= batchSize) {

            writeToFile(path, batch.dequeueAll(_ => true).mkString("\n")) match {
              case scala.util.Success(_) =>
              case scala.util.Failure(exception) =>
                println(s"An error occurred while writing to $path: ${exception.getMessage}")
                promise.failure(exception)
                failStage(exception)
            }
          }
          pull(inPort)
        }

        //need to flush resting batch once upstream finished
        override def onUpstreamFinish(): Unit =  {
          if (batch.nonEmpty) {
            writeToFile(path, batch.dequeueAll(_ => true).mkString("\n")) match {
              case scala.util.Success(_) =>
                promise.success(Done)
                println(s"Finished writing to $path successfully.")
              case scala.util.Failure(exception) =>
                promise.failure(exception)
                println(s"An error occurred: ${exception.getMessage}")
                failStage(exception)
            }
          } else { //upstream is done and everything was already written => Can fullfill promise
            promise.success(Done)
            println(s"Finished writing to $path successfully.")
          }
          super.onUpstreamFinish()
        }
      })
    }
    (logic, promise.future)
  }
}
