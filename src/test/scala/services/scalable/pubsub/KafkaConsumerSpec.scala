package services.scalable.pubsub

import io.vertx.scala.core.Vertx
import io.vertx.scala.kafka.client.consumer.{KafkaConsumer, KafkaConsumerRecords}
import io.vertx.scala.kafka.client.producer.KafkaProducer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Promise}
import scala.io.StdIn
import scala.util.{Failure, Success}

class KafkaConsumerSpec extends AnyFlatSpec {

  "it " should "produce successfully" in {

    val logger = LoggerFactory.getLogger(this.getClass)

    val vertx = Vertx.vertx()

    val cconfig = scala.collection.mutable.Map[String, String]()

    cconfig += (ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "pkc-lgk0v.us-west1.gcp.confluent.cloud:9092")
    cconfig += (ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer")
    cconfig += (ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    cconfig += (ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest")
    cconfig += (ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false")
    cconfig += (ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG -> "60000")
    //cconfig += (ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG -> "10")
    cconfig += (ConsumerConfig.GROUP_ID_CONFIG -> "data_consumer_c0")
    cconfig += (ConsumerConfig.MAX_POLL_RECORDS_CONFIG -> "100")
    cconfig += (ConsumerConfig.FETCH_MAX_BYTES_CONFIG -> (64 * 1024).toString)
    cconfig += ("security.protocol" -> "SASL_SSL")
    cconfig += ("sasl.jaas.config" -> "org.apache.kafka.common.security.plain.PlainLoginModule   required username='ARFOU3FNJAN5S3OG'   password='i8j4Jjc7mirvYHM3PCu+EcOcKmK0nCSV/k2kb+v5dFhF86+EA7fvH+voGHTeUkwW';")
    cconfig += ("sasl.mechanism" -> "PLAIN")
    cconfig += ("client.dns.lookup" -> "use_all_dns_ips")

    val consumer = KafkaConsumer.create[String, Array[Byte]](vertx, cconfig)

    consumer.subscribe("data")

    var pr = Promise[Boolean]()

    def handler(recs: KafkaConsumerRecords[String, Array[Byte]]): Unit = {
      consumer.pause()

      val cmds = (0 until recs.size()).map { i =>
        val rec = recs.recordAt(i)
        new String(rec.value())
      }

      logger.info(s"\n${Console.GREEN_B}data: ${cmds}\n${Console.RESET}")

      /*consumer.commitFuture().onComplete {
        case Success(ok) => consumer.resume()
        case Failure(ex) => ex.printStackTrace()
      }*/
      consumer.resume()
    }

    consumer.handler(_ => {})
    consumer.batchHandler(handler)

    Await.ready(pr.future, Duration.Inf)
  }

}
