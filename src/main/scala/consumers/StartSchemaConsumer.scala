package consumers

object StartSchemaConsumer extends App {
  val schemaConsumer = new AkkaStreamConsumer(
    "ogg-schema",
    "localhost:9092",
    "student-app-id"
  )

  schemaConsumer.start()
}
