package services.scalable.pubsub

import com.datastax.oss.driver.api.core.CqlSession
import com.google.protobuf.ByteString
import com.google.protobuf.any.Any
import io.netty.handler.codec.mqtt.MqttQoS
import io.vertx.scala.core.Vertx
import io.vertx.scala.kafka.client.common.TopicPartition
import io.vertx.scala.kafka.client.consumer.{KafkaConsumer, KafkaConsumerRecords}
import io.vertx.scala.kafka.client.producer.{KafkaProducer, KafkaProducerRecord}
import io.vertx.scala.mqtt.messages.MqttUnsubscribeMessage
import io.vertx.scala.mqtt.{MqttEndpoint, MqttServer, MqttServerOptions, MqttTopicSubscription}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.slf4j.LoggerFactory
import services.scalable.pubsub.grpc._

import java.nio.charset.Charset
import java.util.UUID
import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

class Broker(val id: String, val host: String, val port: Int)(implicit val ec: ExecutionContext) {

  val logger = LoggerFactory.getLogger(this.getClass)
    
  val brokerId = s"broker-$id"

  val vertx = Vertx.vertx()

  val pconfig = scala.collection.mutable.Map[String, String]()
  pconfig += (ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> Config.KAFKA_HOST)
  pconfig += (ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer")
  pconfig += (ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArraySerializer")
  pconfig += (ProducerConfig.ACKS_CONFIG -> "1")
  pconfig += (ProducerConfig.BATCH_SIZE_CONFIG -> (64 * 1024).toString)

  // use producer for interacting with Apache Kafka
  val producer = KafkaProducer.create[String, Array[Byte]](vertx, pconfig)

  val cconfig = scala.collection.mutable.Map[String, String]()

  cconfig += (ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> Config.KAFKA_HOST)
  cconfig += (ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer")
  cconfig += (ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArrayDeserializer")
  cconfig += (ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest")
  cconfig += (ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false")
  cconfig += (ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG -> "60000")
  //cconfig += (ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG -> "10")
  //cconfig += (ConsumerConfig.GROUP_ID_CONFIG -> "brokers")
  cconfig += (ConsumerConfig.MAX_POLL_RECORDS_CONFIG -> "100")
  cconfig += (ConsumerConfig.FETCH_MAX_BYTES_CONFIG -> (64 * 1024).toString)

  val consumer = KafkaConsumer.create[String, Array[Byte]](vertx, cconfig)
  consumer.assign(TopicPartition(new TPartition(Broker.TOPIC, id.toInt)))

  val session = CqlSession
    .builder()
    .withConfigLoader(loader)
    .withKeyspace(Config.KEYSPACE)
    .build()

  def insertSubscription(topic: String, sid: String): Future[Boolean] = {
    val now = System.currentTimeMillis()
    val cmd = Subscribe(UUID.randomUUID.toString, topic, id, sid)
    val buf = Any.pack(cmd).toByteArray
    val record = KafkaProducerRecord.create[String, Array[Byte]](Topics.SUBSCRIPTIONS, cmd.id, buf, now, 0)

    producer.sendFuture(record).map(_ => true)
  }

  /*def publish(msgs: Seq[Message]): Future[Boolean] = {
    val pr = Promise[Boolean]()
    //val now = System.currentTimeMillis()

    msgs.foreach { m =>
      val buf = Any.pack(m).toByteArray
      val record = KafkaProducerRecord.create[String, Array[Byte]](Topics.MESSAGES, m.id,
        buf)
      producer.write(record)
    }

    producer.flush(_ => pr.success(true))

    pr.future.recover {case _ => false}
  }*/

  val options = MqttServerOptions()
  options.setHost(host).setPort(port)
  val server = MqttServer.create(vertx, options)
  val endpoints = TrieMap.empty[String, MqttEndpoint]
  val subscriptions = TrieMap.empty[String, TrieMap[MqttEndpoint, MqttTopicSubscription]]

  def publish(msgs: Seq[Task]): Future[Boolean] = {
    val pr = Promise[Boolean]()
    //val now = System.currentTimeMillis()

    msgs.foreach { m =>
      val buf = Any.pack(m).toByteArray
      val record = KafkaProducerRecord.create[String, Array[Byte]](Topics.TASKS, m.id, buf)
      producer.write(record)
    }

    producer.flush(_ => pr.success(true))

    pr.future.recover {case _ => false}
  }

  def endpointHandler(endpoint: MqttEndpoint): Unit = {

    val client = endpoint.clientIdentifier()

    endpoints.put(client, endpoint)

    // accept connection from the remote client
    endpoint.accept(false)

    // shows main connect info
    logger.info(s"MQTT client [${endpoint.clientIdentifier()}] request to connect, clean session = ${endpoint.isCleanSession()}")

    if (endpoint.auth() != null) {
      val auth = endpoint.auth().asJava
      logger.info(s"[username = ${auth.getUsername}, password = ${auth.getPassword}]")
    }
    if (endpoint.will() != null) {
      val will = endpoint.will().asJava
      logger.info(s"[will topic = ${will.getWillTopic} msg = ${if(will.getWillMessageBytes != null) new String(will.getWillMessageBytes) else null} QoS = ${will.getWillQos} isRetain = ${will.isWillRetain}]")
    }

    logger.info(s"[keep alive timeout = ${endpoint.keepAliveTimeSeconds()}]")

    endpoint.subscribeHandler(sub => {
      var topics = Seq.empty[String]

      sub.topicSubscriptions.foreach(s => {
        subscriptions.get(s.topicName()) match {
          case None => subscriptions.put(s.topicName(), TrieMap(endpoint -> s))
          case Some(topic) => topic.put(endpoint, s)
        }

        topics = topics :+ s.topicName()
      })

      topics = topics.distinct

      Future.sequence(topics.map{insertSubscription(_, client)}).onComplete {
        case Success(ok) => logger.info(s"subscriptions ${topics} for broker ${id} inserted!!!\n")
        case Failure(ex) => ex.printStackTrace()
      }
    })

    endpoint.unsubscribeHandler((unsubscribe: MqttUnsubscribeMessage) => {

      logger.info(s"client ${client} unsubscriptions: ${unsubscribe.topics()}...")

      /*unsubscribe.topics().foreach(t => {
        subscriptions.get(t) match {
          case None =>
          case Some(topics) => topics.remove(endpoint)
        }
      })*/

      // ack the subscriptions request
      endpoint.unsubscribeAcknowledge(unsubscribe.messageId())
    })

    endpoint.publishHandler(message => {

      logger.info(s"${Console.GREEN_B}$brokerId received [${message.payload().toString(Charset.defaultCharset())}] with QoS [${message.qosLevel()}]${Console.RESET}")

      val bmsg = Message(UUID.randomUUID.toString, message.topicName(),
        ByteString.copyFrom(message.payload().getBytes))

      val task = Task(bmsg.id, Some(bmsg))

      publish(Seq(task)).onComplete {
        case Success(ok) =>

          if (message.qosLevel() == MqttQoS.AT_LEAST_ONCE) {
            endpoint.publishAcknowledge(message.messageId())
          } else if (message.qosLevel() == MqttQoS.EXACTLY_ONCE) {
            endpoint.publishReceived(message.messageId())
          }

          logger.info(s"${Console.RED_B}msg ${message} published!!!\n${Console.RESET}")

        case Failure(ex) => ex.printStackTrace()
      }

    }).publishReleaseHandler(msgId => {

      logger.info(s"${Console.BLUE_B}$brokerId released msg $msgId${Console.RESET}")

      endpoint.publishComplete(msgId)
    }).publishCompletionHandler(msgId => {

      logger.info(s"${Console.RED_B}$brokerId completed msg $msgId${Console.RESET}")

    }).publishAcknowledgeHandler(msgId => {
      logger.info(s"${Console.YELLOW_B}$client acked msg $msgId${Console.RESET}")
    })

    // handling disconnect message
    endpoint.disconnectHandler((v: Unit) => {
      logger.info("Received disconnect from client")
    })
  }

  def handler(recs: KafkaConsumerRecords[String, Array[Byte]]): Unit = {
    consumer.pause()

    (0 until recs.size).foreach { i =>
      val rec = recs.recordAt(i)
      val buf = rec.value()
      val post = Any.parseFrom(buf).unpack(Post)

      logger.info(s"${Console.CYAN_B}BROKER: ${post.message.get.topic} => ${post.subscribers}${Console.RESET}\n")

      post.subscribers.foreach { s =>

        endpoints.get(s) match {
          case Some(e) =>

            val msg = post.message.get

            e.publish(msg.topic, io.vertx.core.buffer.Buffer.buffer(msg.data.toByteArray),
              MqttQoS.AT_LEAST_ONCE, false, false)

          case None =>
        }
      }

    }

    consumer.resume()
  }

  consumer.handler(_ => {})
  consumer.batchHandler(handler)

  server.endpointHandler(endpointHandler).listenFuture().onComplete {
    case Success(result) => {
      logger.info(s"${Console.GREEN_B}MQTT server is listening on port ${result.actualPort()}${Console.RESET}")
    }
    case Failure(cause) => {
      logger.info(s"${Console.RED_B}$cause${Console.RESET}")
    }
  }

  def close() = server.closeFuture()

}

object Broker {
  object Config {
    val NUM_BROKERS = 3
  }

  val TOPIC = "services.scalable.pubsub.brokers"

  def compute(id: String): Int = {
    scala.util.hashing.byteswap32(id.##).abs % Config.NUM_BROKERS
  }
}