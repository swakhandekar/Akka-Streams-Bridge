package consumers

import java.nio.ByteBuffer

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.scaladsl.Sink
import akka.stream.{ActorMaterializer, Materializer}
import com.sksamuel.avro4s.{AvroBinaryInputStream, AvroInputStream}
import models.{DBChange, Event, GenericWrapper}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}

import scala.collection.mutable
import scala.concurrent.Future

class AkkaMessageConsumer(private val topic: String,
                          private val bootstrapServer: String,
                          private val groupId: String,
                          private val schemaMap: mutable.Map[Long, String])(implicit val system: ActorSystem) {

  private val DELIMITER = '^'
  private val WAIT_TIME = 2000
  private val consumerSettings: ConsumerSettings[String, Array[Byte]] =
    ConsumerSettings(system, new StringDeserializer, new ByteArrayDeserializer)
      .withBootstrapServers(bootstrapServer)
      .withGroupId(groupId)
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  def start(): Unit = {
    implicit val materializer: Materializer = ActorMaterializer()
    import scala.concurrent.ExecutionContext.Implicits.global

    val done = Consumer.committableSource(consumerSettings, Subscriptions.topics(topic))
      .mapAsync(1)(committableMessage => {
        consumeMessage(committableMessage.record).map(_ => committableMessage)
      })
      .mapAsync(1) { msg => msg.committableOffset.commitScaladsl() }
      .runWith(Sink.ignore)

    done.failed.foreach(throwable => {
      println(throwable)
    })
  }

  private def tryAgain(fingerprint: Long): String = {
    try {
      schemaMap(fingerprint)
    } catch {
      case e: NoSuchElementException => throw new Exception("Schema for message not found even after retries")
    }
  }

  private def getSchemaSafe(fingerprint: Long): String = {
    try {
      schemaMap(fingerprint)
    } catch {
      case e: NoSuchElementException => {
        wait(WAIT_TIME)
        tryAgain(fingerprint)
      }
    }
  }

  private def consumeMessage(record: ConsumerRecord[String, Array[Byte]]): Future[Done] = {
    val messageBytes = record.value()
    var dbChanges: mutable.MutableList[DBChange] = mutable.MutableList()

    val input: AvroBinaryInputStream[GenericWrapper] = AvroInputStream.binary[GenericWrapper](messageBytes)

    input.iterator.toList.foreach(genericWrapper =>  {
      val schemaFingerprint = genericWrapper.schema_fingerprint
      val schema = getSchemaSafe(schemaFingerprint)
      dbChanges += DBChange(genericWrapper.table_name, schema, genericWrapper.payload)
    })

    val event = Event(dbChanges.toList)
    println(event)
    Future.successful(Done)
  }
}
