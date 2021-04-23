package services.scalable.pubsub

import com.datastax.oss.driver.api.core.CqlSession
import com.google.cloud.pubsub.v1.Subscriber
import com.google.cloud.storage.{BlobId, BlobInfo}
import com.google.protobuf.ByteString
import com.google.protobuf.any.Any
import com.google.pubsub.v1.PubsubMessage
import io.netty.handler.codec.mqtt.MqttQoS
import io.vertx.scala.core.Vertx
import io.vertx.scala.kafka.client.consumer.{KafkaConsumer, KafkaConsumerRecords}
import io.vertx.scala.kafka.client.producer.{KafkaProducer, KafkaProducerRecord}
import io.vertx.scala.mqtt.messages.MqttUnsubscribeMessage
import io.vertx.scala.mqtt.{MqttEndpoint, MqttServer, MqttServerOptions, MqttTopicSubscription}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.slf4j.LoggerFactory
import services.scalable.index._
import services.scalable.pubsub.grpc._

import java.nio.ByteBuffer
import java.nio.charset.Charset
import java.util.UUID
import java.util.concurrent.Executor
import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

class Broker(val id: String, val host: String, val port: Int)(implicit val ec: ExecutionContext,
                                                              val cache: Cache[String, Bytes, Bytes],
                                                              val storage: Storage[String, Bytes, Bytes]) {

  val logger = LoggerFactory.getLogger(this.getClass)

  val brokerId = s"broker-$id"

  val session = CqlSession
    .builder()
    .withConfigLoader(loader)
    .withKeyspace(Config.KEYSPACE)
    .build()

  val INSERT_MSG = session.prepare(s"insert into messages(id, data) values(?, ?);")
  val GET_MSG = session.prepare(s"select * from messages where id=?");

  def insertMsg(m: Message): Future[Boolean] = {
    session.executeAsync(INSERT_MSG
      .bind()
      .setString(0, m.id)
      .setByteBuffer(1, ByteBuffer.wrap(Any.pack(m).toByteArray))
    ).map(_.wasApplied())
  }

  def getMsg(mid: String): Future[Option[Message]] = {
    session.executeAsync(GET_MSG
      .bind()
      .setString(0, mid)
    ).map { rs =>
      val one = rs.one()
      if(one == null) None else Some(Any.parseFrom(one.getByteBuffer("data").array()).unpack(Message))
    }
  }

  val vertx = Vertx.vertx(voptions)

  val cconfig = scala.collection.mutable.Map[String, String]()

  cconfig += (ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> Config.KAFKA_HOST)
  cconfig += (ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer")
  cconfig += (ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArrayDeserializer")

  cconfig += (ConsumerConfig.GROUP_ID_CONFIG -> s"broker-$id")

  cconfig += (ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest")
  cconfig += (ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false")
  cconfig += (ConsumerConfig.MAX_POLL_RECORDS_CONFIG -> "1000")
  cconfig += (ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG -> "100")
  cconfig += (ConsumerConfig.FETCH_MIN_BYTES_CONFIG -> (1 * 1024 * 1024).toString)

  val consumer = KafkaConsumer.create[String, Array[Byte]](vertx, cconfig)

  val pconfig = scala.collection.mutable.Map[String, String]()
  pconfig += (ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> Config.KAFKA_HOST)
  pconfig += (ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer")
  pconfig += (ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArraySerializer")
  pconfig += (ProducerConfig.ACKS_CONFIG -> "all")
  pconfig += (ProducerConfig.BATCH_SIZE_CONFIG -> (64 * 1024).toString)

  // use producer for interacting with Apache Kafka
  val producer = KafkaProducer.create[String, Array[Byte]](vertx, pconfig)

  def publishSubscription(topics: Seq[String], client: String): Future[Boolean] = {
    if(topics.isEmpty) return Future.successful(true)

    val pr = Promise[Boolean]()

    topics.foreach { t =>
      val sub = Subscribe(UUID.randomUUID.toString, topics, brokerId, client)
      val buf = Any.pack(sub).toByteArray
      val record = KafkaProducerRecord.create[String, Array[Byte]](s"sub-${computeSubscriptionTopic(t)}", sub.id, buf)

      producer.write(record)
    }

    producer.flush(_ => pr.success(true)).exceptionHandler(ex => pr.failure(ex))

    pr.future
  }

  val options = MqttServerOptions()
  options.setHost(host).setPort(port)
  val server = MqttServer.create(vertx, options)
  val endpoints = TrieMap.empty[String, MqttEndpoint]
  val subscriptions = TrieMap.empty[String, TrieMap[MqttEndpoint, MqttTopicSubscription]]

  def publishTask(task: Task): Future[Boolean] = {
    val buf = Any.pack(task).toByteArray
    val record = KafkaProducerRecord.create[String, Array[Byte]](Topics.TASKS, task.id, buf)

    producer.sendFuture(record).map(_ => true)
  }

  def getRoot(topic: String): Future[Option[String]] = {
    storage.load(s"${topic}_subscribers").map(_.root).recover{case _ => None}
  }

  def endpointHandler(endpoint: MqttEndpoint): Unit = {

    val client = endpoint.clientIdentifier()

    endpoints.put(client, endpoint)
    brokerClients.put(client, id)

    // accept connection from the remote client
    endpoint.accept(false)

    // shows main connect info
    logger.info(s"MQTT client [${endpoint.clientIdentifier()}] request to connect, clean session = ${endpoint.isCleanSession()}")

    /*if (endpoint.auth() != null) {
      val auth = endpoint.auth().asJava
      logger.info(s"[username = ${auth.getUsername}, password = ${auth.getPassword}]")
    }
    if (endpoint.will() != null) {
      val will = endpoint.will().asJava
      logger.info(s"[will topic = ${will.getWillTopic} msg = ${if(will.getWillMessageBytes != null) new String(will.getWillMessageBytes) else null} QoS = ${will.getWillQos} isRetain = ${will.isWillRetain}]")
    }*/

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

      publishSubscription(topics, client).onComplete {
        case Success(ok) =>
          println(s"subscriptions ${topics} for $client inserted!!!\n")
          endpoint.subscribeAcknowledge(sub.messageId(), sub.topicSubscriptions().map{ts => ts.qualityOfService()})

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

      logger.info(s"\n\n${Console.GREEN_B}$brokerId received [${message.payload().toString(Charset.defaultCharset())}] with QoS [${message.qosLevel()}]${Console.RESET}\n\n")

      val mid = UUID.randomUUID.toString
      val data = message.payload().getBytes()

      val m = Message(mid, message.topicName(), ByteString.copyFrom(data), message.qosLevel().value())

      getRoot(m.topic).flatMap(r => insertMsg(m).map(_ => r)).flatMap {
        case None => Future.successful(true)
        case Some(root) => publishTask(Task(mid, message.topicName(), message.qosLevel().value(),
          root))
      }.onComplete {
        case Success(ok) =>

          println(s"${Console.GREEN_B}successfully published message with id ${ok}${Console.RESET}\n")

          if (message.qosLevel() == MqttQoS.AT_LEAST_ONCE) {
            endpoint.publishAcknowledge(message.messageId())
          } else if (message.qosLevel() == MqttQoS.EXACTLY_ONCE) {
            endpoint.publishReceived(message.messageId())
          }

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

  def commitAndResume(): Unit = {
    consumer.commitFuture().onComplete {
      case Success(value) => consumer.resume()
      case Failure(ex) => throw ex
    }
  }

  def handler(recs: KafkaConsumerRecords[String, Array[Byte]]): Unit = {
    consumer.pause()

    val posts = (0 until recs.size()).map { i =>
      val rec = recs.recordAt(i)
      Any.parseFrom(rec.value()).unpack(Post)
    }

    Future.sequence(posts.map{p => getMsg(p.id).map(p -> _)}).onComplete {
      case Success(posts) =>

        posts.filter(_._2.isDefined).foreach { case (post, opt) =>

          val m = opt.get

          logger.info(s"${Console.MAGENTA_B}$brokerId processing m ${m.id} => ${post.subscribers}${Console.RESET}\n")

          post.subscribers.foreach { s =>

            endpoints.get(s) match {
              case Some(e) =>

                e.publish(m.topic, io.vertx.core.buffer.Buffer.buffer(m.data.toByteArray), MqttQoS.valueOf(post.qos),
                  false, false)

              case None =>
            }
          }
        }

        commitAndResume()

      case Failure(ex) => logger.info(ex.getMessage())
    }
  }

  server.endpointHandler(endpointHandler).listenFuture().onComplete {
    case Success(result) => {
      logger.info(s"${Console.GREEN_B}MQTT server is listening on port ${result.actualPort()}${Console.RESET}")

      consumer.subscribeFuture(s"broker-$id").onComplete {
        case Success(ok) =>

          consumer.handler(_ => {})
          consumer.batchHandler(handler)

        case Failure(ex) => throw ex
      }

    }
    case Failure(cause) => {
      logger.info(s"${Console.RED_B}$cause${Console.RESET}")
    }
  }

  def close() = {
    producer.close()
    consumer.close()

    vertx.close()
    server.closeFuture()
  }

}