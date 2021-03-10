package services.scalable.pubsub

import io.netty.handler.codec.mqtt.MqttQoS
import io.vertx.core.AsyncResult
import io.vertx.scala.core.Vertx
import io.vertx.scala.mqtt.{MqttClient, MqttClientOptions}
import io.vertx.scala.mqtt.messages.MqttPublishMessage

import scala.concurrent.ExecutionContext.Implicits.global
import java.util.concurrent.ThreadLocalRandom
import scala.util.{Failure, Success}

object BrokerClient {

  def main(args: Array[String]): Unit = {

    val rand = ThreadLocalRandom.current()
    val vertx = Vertx.vertx()

    val topics = scala.collection.mutable.Map(
      "demo" -> MqttQoS.AT_MOST_ONCE.value(),
      "test" -> MqttQoS.AT_LEAST_ONCE.value()
    )

    for(i<-0 until 20){

      val cid = s"c${i}"
      val group = rand.nextInt(0, Broker.Config.NUM_BROKERS)
      val port = 3000 + group

      println(s"${Console.GREEN_B}client-$cid group: ${group}${Console.RESET}\n")

      val options = MqttClientOptions().setClientId(cid)
      val client = MqttClient.create(vertx, options)

      //val external_ip = "191.220.109.193"

      client.connectFuture(port, "192.168.1.66").onComplete {
        case Success(ok) =>

          client.publishHandler((s: MqttPublishMessage) => {
            //if(topics.isDefinedAt(s.topicName())){
            println(s"client ${cid} received message in topic ${s.topicName()}: ${s.payload().toString()} message id ${s.messageId()}")
            //}
          })

          /*client.unsubscribeCompletionHandler((event: Int) => {
            println(s"${Console.YELLOW_B}client ${cid} has unsubed!${Console.RESET}")
          })*/

          client.subscribe("test", MqttQoS.AT_LEAST_ONCE.value(), (event: AsyncResult[Int]) => {
            if(event.succeeded()){
              println(s"successfully subed to topic test...")
            } else {
              event.cause().printStackTrace()
            }
          })

          /*client.subscribe("demo", MqttQoS.AT_LEAST_ONCE.value(), (event: AsyncResult[Int]) => {
            if(event.succeeded()){
              println(s"successfully subed to topic demo...")
            } else {
              event.cause().printStackTrace()
            }
          })*/

        case Failure(ex) => ex.printStackTrace()
      }

    }

  }

}
