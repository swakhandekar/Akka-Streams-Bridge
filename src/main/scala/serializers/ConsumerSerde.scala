package serializers

import com.lightbend.kafka.scala.streams.DefaultSerdes._
import com.sksamuel.avro4s.{FromRecord, SchemaFor, ToRecord}
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.Consumed

import scala.reflect.ClassTag

object ConsumerSerde {

  def generateSerde[T: ClassTag : SchemaFor : ToRecord : FromRecord](): Serde[T] = {
    Serdes.serdeFrom(
      new GenericSerializer[T](),
      new GenericDeserializer[T]()
    )
  }

  implicit val readPayloadConsumed: Consumed[String, Array[Byte]] = Consumed.`with`(stringSerde, byteArraySerde)
}

