package consumers

import java.util.Properties

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

object Bridge extends App {
  val consumer = new Consumer()
  consumer.groupByTxId()


  val config: Properties = {
    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, s"StudentAdmission")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Bytes().getClass)
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Bytes().getClass)
    props
  }

  private val builder = BuilderFactory.getBuilder()
  val streams: KafkaStreams = new KafkaStreams(builder.build(), config)
  streams.start()
}
