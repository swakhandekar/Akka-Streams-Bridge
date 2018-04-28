import com.lightbend.kafka.scala.streams.KStreamS
import org.apache.avro.SchemaNormalization.fingerprint64

class Consumer {
  private def readSchema(): KStreamS[String, String] = {
    import serde.ConsumerSerde.readPayloadConsumed
    val builder = BuilderFactory.getBuilder()
    builder.stream[String, String]("ogg-schema")
  }

  def transformSchemaStream(): KStreamS[Long, String] = {
    val schemaStream: KStreamS[String, String] = readSchema()
    schemaStream.map((_, schema) => {
      val schemaFingerprint = fingerprint64(schema.getBytes)
      (schemaFingerprint, schema)
    })
  }
}
