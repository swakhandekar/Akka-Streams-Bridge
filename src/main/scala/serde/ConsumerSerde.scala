package serde

import com.lightbend.kafka.scala.streams.DefaultSerdes._
import org.apache.kafka.streams.Consumed
import org.apache.kafka.streams.kstream.{Joined, Serialized}

object ConsumerSerde {
  implicit val readSchemaConsumed = Consumed.`with`(stringSerde, stringSerde)
  implicit val readPayloadConsumed = Consumed.`with`(stringSerde, stringSerde)
  implicit val joinSchemaPayloadSerde = Joined.`with`(longSerde, stringSerde, stringSerde)
  implicit val serializedLongString = Serialized.`with`(longSerde, stringSerde)
}
