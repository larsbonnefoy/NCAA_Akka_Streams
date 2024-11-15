package NcaaPipeFilter

import akka.stream.scaladsl.GraphDSL
import akka.NotUsed
import akka.stream.scaladsl.Flow
import akka.stream.SinkShape
import akka.stream.scaladsl.Sink

class CustomSink[T]

object CustomSink {

  /**
  * @param flow is applied before elements are passed to a BatchWriter
  * @param fileName is the file name to which the sink writes
  * @param batchSize is the number of elements written at once
  */
  def apply[T](flow: Flow[T, T, Any], fileName: String, batchSize: Int = 20) = 
    Sink.fromGraph(
      GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>
        import GraphDSL.Implicits._

        val converterFlow = builder.add(flow)

        val sink = builder.add(Sink.fromGraph(new BatchWriter(fileName, batchSize)))

        converterFlow ~> sink

        SinkShape(converterFlow.in)
      }
    )
}

