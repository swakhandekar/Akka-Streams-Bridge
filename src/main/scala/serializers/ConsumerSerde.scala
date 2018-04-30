package serializers

import com.lightbend.kafka.scala.streams.DefaultSerdes._
import models.GenericWrapper
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.Consumed
import org.apache.kafka.streams.kstream.{Joined, Serialized}

object ConsumerSerde {
  val genericWrapperSerde: Serde[GenericWrapper] = Serdes.serdeFrom(
    new GenericSerializer[GenericWrapper](),
    new GenericDeserializer[GenericWrapper]()
  )

  implicit val readSchemaConsumed: Consumed[String, String] = Consumed.`with`(stringSerde, stringSerde)
  implicit val readPayloadConsumed: Consumed[String, Array[Byte]] = Consumed.`with`(stringSerde, byteArraySerde)
  implicit val joinSchemaPayloadSerde: Joined[Long, GenericWrapper, String] = Joined.`with`(longSerde, genericWrapperSerde, stringSerde)
  implicit val serializedLongString: Serialized[Long, String] = Serialized.`with`(longSerde, stringSerde)
}

