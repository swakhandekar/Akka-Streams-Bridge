package serde

import com.lightbend.kafka.scala.streams.DefaultSerdes._
import org.apache.kafka.streams.Consumed

object AllSerde {
  implicit val readPayloadConsumed = Consumed.`with`(stringSerde, stringSerde)
}
