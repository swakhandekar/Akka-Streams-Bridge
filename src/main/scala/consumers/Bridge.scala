package consumers

import java.util.Properties

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

import scala.collection.concurrent.TrieMap
import scala.collection.mutable

object Bridge extends App {
  val schemaMap: mutable.Map[Long, String] = TrieMap()

  def startSchemaConsumer(): Unit ={
    val schemaConsumer = new AkkaStreamConsumer(
      "ogg-schema",
      "localhost:9092",
      "student-app-id",
      schemaMap
    )
    schemaConsumer.start()
  }

  def startMessageConsumer(): Unit = {
    val messageConsumer = new MessageConsumer(schemaMap)
    messageConsumer.start()
  }

  val consumerConfig: Properties = {
    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, s"StudentAdmission")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Bytes().getClass)
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Bytes().getClass)
    props
  }

  startSchemaConsumer()
  startMessageConsumer()
  private val builder = BuilderFactory.getBuilder()
  val streams: KafkaStreams = new KafkaStreams(builder.build(), consumerConfig)
  streams.start()
}
