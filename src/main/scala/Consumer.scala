import com.lightbend.kafka.scala.streams.KStreamS

class Consumer {
  def readSchema(): Unit = {
    import serde.ConsumerSerde.readPayloadConsumed
    val builder = BuilderFactory.getBuilder()
    val schemaStream: KStreamS[String, String] = builder.stream[String, String]("ogg-schema")

    schemaStream.peek((_, value) => println(value))
  }
}
