package serde

import java.io.ByteArrayOutputStream
import java.util

import com.sksamuel.avro4s.{AvroOutputStream, FromRecord, SchemaFor, ToRecord}
import org.apache.kafka.common.serialization.Serializer

import scala.reflect.ClassTag

class GenericSerializer[T: ClassTag : SchemaFor : ToRecord : FromRecord] extends Serializer[T] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def serialize(topic: String, data: T): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val output = AvroOutputStream.binary[T](baos)
    output.write(data)
    output.close()
    baos.toByteArray
  }

  override def close(): Unit = {}
}
