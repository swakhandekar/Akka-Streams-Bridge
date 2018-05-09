package consumers

import java.util.concurrent.atomic.AtomicLong

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscription, Subscriptions}
import akka.stream.{ActorMaterializer, Materializer}
import akka.stream.scaladsl.Sink
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer

import scala.concurrent.Future

class AkkaStreamConsumer(private val topic: String,
                         private val bootstrapServer: String,
                         private val groupId: String) {

  private implicit val system: ActorSystem = ActorSystem()

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
    println(record.value())
    offset.set(record.offset)
    Future.successful(Done)
  }
}
