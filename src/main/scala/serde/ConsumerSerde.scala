package serde

import com.lightbend.kafka.scala.streams.DefaultSerdes._
import org.apache.kafka.streams.Consumed

object ConsumerSerde {
  implicit val readSchemaConsumed = Consumed.`with`(stringSerde, stringSerde)
  implicit val readPayloadConsumed = Consumed.`with`(stringSerde, stringSerde)
}
