package serializers

import com.lightbend.kafka.scala.streams.DefaultSerdes._
import com.sksamuel.avro4s.{FromRecord, SchemaFor, ToRecord}
import models.{DBChange, Event, Message}
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.Consumed
import org.apache.kafka.streams.kstream.{Joined, Serialized}

import scala.reflect.ClassTag

object ConsumerSerde {

  def generateSerde[T: ClassTag : SchemaFor : ToRecord : FromRecord](): Serde[T] = {
    Serdes.serdeFrom(
      new GenericSerializer[T](),
      new GenericDeserializer[T]()
    )
  }

  val messageSerde: Serde[Message] = generateSerde[Message]()
  val dbChangeSerde: Serde[DBChange] = generateSerde[DBChange]()
  val eventSerde: Serde[Event] = generateSerde[Event]()

  implicit val readSchemaConsumed: Consumed[String, String] = Consumed.`with`(stringSerde, stringSerde)
  implicit val readPayloadConsumed: Consumed[String, Array[Byte]] = Consumed.`with`(stringSerde, byteArraySerde)
  implicit val joinSchemaMessage: Joined[Long, Message, String] = Joined.`with`(longSerde, messageSerde, stringSerde)
  implicit val serializedLongString: Serialized[Long, String] = Serialized.`with`(longSerde, stringSerde)
  implicit val serializedStringDBChange: Serialized[String, DBChange] = Serialized.`with`(stringSerde, dbChangeSerde)
}

