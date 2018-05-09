package consumers

import akka.actor.ActorSystem

import scala.collection.concurrent.TrieMap
import scala.collection.mutable

object Bridge extends App {
  val schemaMap: mutable.Map[Long, String] = TrieMap()

  val bootStrapServer = "localhost:9092"
  val groupId = "student-app-id"

  implicit val system: ActorSystem = ActorSystem()

  def startSchemaConsumer(): Unit = {
    val schemaConsumer = new AkkaSchemaConsumer(
      "ogg-schema",
      bootStrapServer,
      groupId,
      schemaMap
    )
    schemaConsumer.start()
  }

  def startMessageConsumer(): Unit = {
    val messageConsumer = new AkkaMessageConsumer(
      "ogg-payload",
      bootStrapServer,
      groupId,
      schemaMap
    )
    messageConsumer.start()
  }

  startSchemaConsumer()
  startMessageConsumer()
}
