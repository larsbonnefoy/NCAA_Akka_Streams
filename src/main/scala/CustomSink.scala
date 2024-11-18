package NcaaPipeFilter

import akka.stream.scaladsl.GraphDSL
import akka.NotUsed
import akka.stream.scaladsl.Flow
import akka.stream.SinkShape
import akka.stream.scaladsl.Sink
import akka.stream.Graph
import scala.concurrent.Future
import akka.Done
import akka.stream.javadsl.Source

class CustomSink[T]

object CustomSink {

  /**
  * @param flow is applied before elements are passed to a BatchWriter
  * @param fileName is the file name to which the sink writes
  * @param batchSize is the number of elements written at once
  */
  def apply[T](flow: Flow[T, T, Any], fileName: String, batchSize: Int = 20): Sink[T, Future[Done]] = {
    val batchwriter = Sink.fromGraph(new BatchWriter[T](fileName, batchSize))
    val g = Sink.fromGraph(
      GraphDSL.create(batchwriter) { implicit builder => sinkShape => //Issue is that sinkShape is of type Shape => more general than sinkShape
        import GraphDSL.Implicits._

        val converterShape = builder.add(flow)
        val sink : SinkShape[T] = sinkShape.asInstanceOf[SinkShape[T]] //Terrible CAST !! idk how do it here

        converterShape ~> sink

        SinkShape(converterShape.in)
      }
    )
    g
  }
}

