import java.util.Properties

import com.lightbend.kafka.scala.streams.{KStreamS, StreamsBuilderS}
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

object Consumer extends App {
  val builder = new StreamsBuilderS

  def readSchema(): Unit = {
    import serde.AllSerde.readPayloadConsumed
    val schemaStream: KStreamS[String, String] = builder.stream[String, String]("ogg-schema")

    schemaStream.peek((_, value) => println(value))
  }

  readSchema()

  val config: Properties = {
    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application-3")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass)
    props
  }

  val streams: KafkaStreams = new KafkaStreams(builder.build(), config)
  streams.start()
}
