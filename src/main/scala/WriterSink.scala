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


//back pressure will work automatically
class  WriterSink[T](path: String, batchSize: Int) extends GraphStage[SinkShape[T]] {

  val inPort = Inlet[T]("writer")

  override def shape: SinkShape[T] = SinkShape[T](inPort)

  def writeToFile(filePath: String, str: String): Try[Unit] = {
    Using(new FileWriter(path, true)) { writer =>
      writer.write(str + "\n")
      writer.close()
    }
  }

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
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
          }
        }
        pull(inPort)
      }

      //need to flush resting batch once upstream finished
      override def onUpstreamFinish(): Unit =  {
        if (batch.nonEmpty) {
          writeToFile(path, batch.dequeueAll(_ => true).mkString("\n")) match {
            case scala.util.Success(_) =>
              println(s"Finished writing to $path successfully.")
            case scala.util.Failure(exception) =>
              println(s"An error occurred: ${exception.getMessage}")
          }
        }
      }
    })
  }
}
