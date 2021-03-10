package services.scalable.pubsub

import io.vertx.scala.core.Vertx
import io.vertx.scala.kafka.client.producer.{KafkaProducer, KafkaProducerRecord}
import org.apache.kafka.clients.producer.ProducerConfig
import org.scalatest.flatspec.AnyFlatSpec
import services.scalable.pubsub.grpc._
import com.google.protobuf.any.Any
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits.global
import java.util.UUID
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, Promise}

class CommandSpec extends AnyFlatSpec {

  val logger = LoggerFactory.getLogger(this.getClass)

  /*"it " should "publish commands in subscribers kafka topic" in {

    val vertx = Vertx.vertx()

    val pconfig = scala.collection.mutable.Map[String, String]()
    pconfig += (ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> Config.KAFKA_HOST)
    pconfig += (ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer")
    pconfig += (ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArraySerializer")
    pconfig += (ProducerConfig.ACKS_CONFIG -> "1")
    pconfig += (ProducerConfig.BATCH_SIZE_CONFIG -> (64 * 1024).toString)

    // use producer for interacting with Apache Kafka
    val producer = KafkaProducer.create[String, Array[Byte]](vertx, pconfig)

    var subscriptions = Seq.empty[Subscribe]

    def post(subscriptions: Seq[Subscribe]): Future[Boolean] = {

      if(subscriptions.isEmpty) return Future.successful(true)

      val pr = Promise[Boolean]()
      //val now = System.currentTimeMillis()

      subscriptions.foreach { s =>
        val buf = Any.pack(s).toByteArray
        val record = KafkaProducerRecord.create[String, Array[Byte]](Topics.SUBSCRIPTIONS, s.id, buf)
        producer.write(record)
      }

      producer.flush(_ => pr.success(true))

      pr.future.recover {case _ => false}
    }

    for(i<-0 until 10) {
      val subscriber = s"subscriber-$i"

      val s = Subscribe(UUID.randomUUID.toString, s"topic-$i", "mqtt", subscriber)
      subscriptions :+= s
    }

    val result = Await.result(post(subscriptions), Duration.Inf)

    logger.info(s"result: ${result}")
  }*/

}
