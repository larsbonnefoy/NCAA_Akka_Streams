package NcaaPipeFilter

class CustomSink[T]

object CustomSink {

  // /**
  // * @param path is the path to a csv file
  // * @param transform is the function used to parse each row. Key of Map is the header of the column, Value of Map is entry in that column
  // */
  // def apply[T](path: String)(transform: Map[String, String] => T) = GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>
  //     import GraphDSL.Implicits._
  //
  //     val file = Paths.get(path)
  //
  //     //Produces a mapping from lines of the CSV file to Map[String, String]
  //     val csvToMapReader = FileIO
  //                     .fromPath(file)
  //                     .via(CsvParsing.lineScanner())
  //                     .via(CsvToMap.toMapAsStrings())
  //
  //     //Converts Map[String, String] to object of type T
  //     val converterFlow = Flow[Map[String, String]].map[T](transform)
  //
  //     val source = builder.add(csvToMapReader)
  //     val transformflow = builder.add(converterFlow)
  //
  //     source ~> transformflow
  //
  //     SourceShape(transformflow.out)
  //   }
}

