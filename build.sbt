name := "Kafka-Streams_Bridge"

version := "0.1"

scalaVersion := "2.12.6"

libraryDependencies ++= Seq(
  "com.sksamuel.avro4s" %% "avro4s-core" % "1.8.3",
  "com.typesafe.akka" %% "akka-stream-kafka" % "0.20"
)