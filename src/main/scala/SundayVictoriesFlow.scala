package NcaaPipeFilter

import akka.stream.scaladsl.GraphDSL
import akka.NotUsed
import akka.stream.FlowShape
import akka.stream.scaladsl.Flow

class SundayVictoriesFlow[I, O]

//TODO: First do a simple flow without splitting in two
//TODO: make two flows, from I to O and then count O
object  SundayVictoriesFlow {

  /**
  * @param neutral is the neutral element of type 0
  * @param transform is the function used to change elements of type I to type O and 
  */
 def apply[I, O](neutral: O)(fold: (O, I) => O) = GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>
      import GraphDSL.Implicits._

      //Converts Map[String, String] to object of type T
      val foldFlow = Flow[I].fold[O](neutral)(fold)

      // val source = builder.add(csvToMapReader)
      val transformflow = builder.add(foldFlow)

      FlowShape(transformflow.in, transformflow.out)
    }
}
