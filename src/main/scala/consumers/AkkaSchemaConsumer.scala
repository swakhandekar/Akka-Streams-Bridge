package consumers

import java.util.concurrent.atomic.AtomicLong

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscription, Subscriptions}
import akka.stream.{ActorMaterializer, Materializer}
import akka.stream.scaladsl.Sink
import org.apache.avro.{Schema, SchemaNormalization}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.mutable
import scala.concurrent.Future

class AkkaSchemaConsumer(private val topic: String,
                         private val bootstrapServer: String,
                         private val groupId: String,
                         private val sink: mutable.Map[Long, String])(implicit val system: ActorSystem) {

  private val consumerSettings: ConsumerSettings[String, String] =
    ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
    .withBootstrapServers(bootstrapServer)
    .withGroupId(groupId)
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  private val offset: AtomicLong = new AtomicLong

  private def subscribeTopicWithZeroOffset(): Subscription = {
    Subscriptions
      .assignmentWithOffset(new TopicPartition(topic, 0), offset.get())
  }

  def start(): Unit = {
    implicit val materializer: Materializer = ActorMaterializer()

    val subscription: Subscription = subscribeTopicWithZeroOffset()
    Consumer.plainSource(consumerSettings, subscription)
      .mapAsync(1)(consumeSchema)
      .runWith(Sink.ignore)
  }

  private def consumeSchema(record: ConsumerRecord[String, String]): Future[Done] ={
    val rawSchema = record.value()
    val schema =  new Schema.Parser().parse(rawSchema)
    val fingerprint = SchemaNormalization.parsingFingerprint64(schema)
    sink.put(fingerprint, rawSchema)
    offset.set(record.offset)
    Future.successful(Done)
  }
}
