name := "Kafka-Streams_Bridge"

version := "0.1"

scalaVersion := "2.12.6"

libraryDependencies ++= Seq(
  "com.sksamuel.avro4s" %% "avro4s-core" % "1.8.3",
  "org.apache.avro" % "avro" % "1.8.2",
  "com.lightbend" %% "kafka-streams-scala" % "0.2.1"
)