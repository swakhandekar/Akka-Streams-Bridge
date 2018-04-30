package serde

import java.io.ByteArrayInputStream
import java.util

import com.sksamuel.avro4s.{AvroInputStream, FromRecord, SchemaFor, ToRecord}
import org.apache.kafka.common.serialization.Deserializer

import scala.reflect.ClassTag

class GenericDeserializer[T : ClassTag : SchemaFor : ToRecord : FromRecord] extends Deserializer[T]{
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def close(): Unit = {}

  override def deserialize(topic: String, data: Array[Byte]): T = {
    val in = new ByteArrayInputStream(data)
    val input = AvroInputStream.binary[T](in)
    val result = input.iterator.next()
    result
  }
}
