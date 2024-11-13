val scala3Version = "3.5.2"
val AkkaVersion = "2.10.0"

resolvers += "Akka library repository" at "https://repo.akka.io/maven"

libraryDependencies ++= Seq(
  "com.lightbend.akka" %% "akka-stream-alpakka-csv" % "9.0.0",
  "com.typesafe.akka" %% "akka-stream" % AkkaVersion
)

lazy val root = project
  .in(file("."))
  .settings(
    name := "SA_1_Akka_Streams",
    version := "0.1.0-SNAPSHOT",

    scalaVersion := scala3Version,

    libraryDependencies += "org.scalameta" %% "munit" % "1.0.0" % Test
  )

fork in run := true
