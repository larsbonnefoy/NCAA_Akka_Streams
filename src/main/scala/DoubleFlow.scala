package NcaaPipeFilter

import akka.stream.scaladsl.GraphDSL
import akka.NotUsed
import akka.stream.FlowShape
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Balance
import akka.stream.scaladsl.Merge

class DoubleFlow[I, O]

//TODO: First do a simple flow without splitting in two
//TODO: make two flows, from I to O and then count O
object  DoubleFlow {

  /**
   * Splits incoming source into Two. 
   * Applies same function fold to each split.
   * Merges back together stream
   *
  * @param neutral is the neutral element of type 0
  * @param fold is the function used to change elements of type I to type O and 
  */
 def apply[I, O](neutral: O)(fold: (O, I) => O) = GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>
      import GraphDSL.Implicits._

      //Converts Map[String, String] to object of type T
      val foldFlow = Flow[I].fold[O](neutral)(fold).log("LoggingFoldFlow")

      val balanceShape = builder.add(Balance[I](2))
      val mergeShape = builder.add(Merge[O](2))

      // val source = builder.add(csvToMapReader)
      val foldShape1 = builder.add(foldFlow)
      val foldShape2 = builder.add(foldFlow)

      balanceShape ~> foldShape1 ~> mergeShape
      balanceShape ~> foldShape2 ~> mergeShape

      FlowShape(balanceShape.in, mergeShape.out)
    }
}
