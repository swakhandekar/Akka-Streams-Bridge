package serializers

import com.lightbend.kafka.scala.streams.DefaultSerdes._
import com.sksamuel.avro4s.{FromRecord, SchemaFor, ToRecord}
import models.DBChange
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

  val dbChangeSerde: Serde[DBChange] = generateSerde[DBChange]()

  implicit val readSchemaConsumed: Consumed[String, String] = Consumed.`with`(stringSerde, stringSerde)
  implicit val readPayloadConsumed: Consumed[String, Array[Byte]] = Consumed.`with`(stringSerde, byteArraySerde)
  implicit val joinSchemaDBChange: Joined[Long, DBChange, String] = Joined.`with`(longSerde, dbChangeSerde, stringSerde)
  implicit val serializedLongString: Serialized[Long, String] = Serialized.`with`(longSerde, stringSerde)
}

