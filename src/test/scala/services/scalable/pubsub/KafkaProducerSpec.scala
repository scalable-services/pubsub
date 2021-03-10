package services.scalable.pubsub

import io.vertx.scala.core.Vertx
import io.vertx.scala.kafka.client.producer.{KafkaProducer, KafkaProducerRecord}
import org.apache.kafka.clients.producer.ProducerConfig
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory

import java.util.UUID
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}

class KafkaProducerSpec extends AnyFlatSpec {

  "it " should "produce successfully" in {

    val logger = LoggerFactory.getLogger(this.getClass)

    val vertx = Vertx.vertx()

    val pconfig = scala.collection.mutable.Map[String, String]()
    pconfig += (ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> "pkc-lgk0v.us-west1.gcp.confluent.cloud:9092")
    pconfig += (ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer")
    pconfig += (ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArraySerializer")
    pconfig += (ProducerConfig.ACKS_CONFIG -> "1")
    pconfig += (ProducerConfig.BATCH_SIZE_CONFIG -> (64 * 1024).toString)
    pconfig += (ProducerConfig.CLIENT_DNS_LOOKUP_CONFIG -> "use_all_dns_ips")
    pconfig += ("security.protocol" -> "SASL_SSL")
    pconfig += ("sasl.jaas.config" -> "org.apache.kafka.common.security.plain.PlainLoginModule   required username='ARFOU3FNJAN5S3OG'   password='i8j4Jjc7mirvYHM3PCu+EcOcKmK0nCSV/k2kb+v5dFhF86+EA7fvH+voGHTeUkwW';")
    pconfig += ("sasl.mechanism" -> "PLAIN")
    pconfig += ("client.dns.lookup" -> "use_all_dns_ips")

    // use producer for interacting with Apache Kafka
    val producer = KafkaProducer.create[String, Array[Byte]](vertx, pconfig)

    def post(list: Seq[String]): Future[Boolean] = {
      val pr = Promise[Boolean]()
      val now = System.currentTimeMillis()

      list.foreach { s =>
        val buf = s.getBytes()
        val record = KafkaProducerRecord.create[String, Array[Byte]]("data",
          UUID.randomUUID.toString, buf)

        producer.write(record)
      }

      producer.flush(_ => pr.success(true))

      pr.future
    }

    var data = Seq.empty[String]

    val n = 100

    for(i<-0 until n){
      data :+= i.toString
    }

    logger.info(s"${Await.result(post(data), Duration.Inf)}")

    /*val vertx = Vertx.vertx()

    val properties = new Properties()
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
    properties.load(new FileReader("kafka.properties"))

    val producer = KafkaProducer.create[String, Array[Byte]](vertx, properties)*/

  }

}
