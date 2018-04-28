import com.lightbend.kafka.scala.streams.StreamsBuilderS

object BuilderFactory {
  private var builder: StreamsBuilderS = _

  private def instantiateBuilder(): Unit ={
    builder = new StreamsBuilderS
  }

  def getBuilder(): StreamsBuilderS ={
    if(builder == null) instantiateBuilder()
    builder
  }
}
