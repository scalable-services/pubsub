package services.scalable.pubsub

import com.datastax.oss.driver.api.core.CqlSession
import com.google.api.gax.core.FixedCredentialsProvider
import com.google.cloud.pubsub.v1.{AckReplyConsumer, MessageReceiver, Publisher, Subscriber}
import com.google.cloud.storage.{BlobId, BlobInfo, StorageOptions}
import com.google.protobuf.ByteString
import com.google.protobuf.any.Any
import com.google.pubsub.v1.{ProjectSubscriptionName, PubsubMessage, TopicName}
import io.netty.handler.codec.mqtt.MqttQoS
import io.vertx.scala.core.Vertx
import io.vertx.scala.kafka.client.consumer.KafkaConsumerRecords
import io.vertx.scala.kafka.client.producer.KafkaProducerRecord
import io.vertx.scala.mqtt.messages.MqttUnsubscribeMessage
import io.vertx.scala.mqtt.{MqttEndpoint, MqttServer, MqttServerOptions, MqttTopicSubscription}
import org.slf4j.LoggerFactory
import services.scalable.index._
import services.scalable.pubsub.grpc._

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

  val subscriptionId = s"broker-$id-sub"
  val subscriptionName = ProjectSubscriptionName.of(Config.projectId, subscriptionId)

  val taskPublisher = Publisher
    .newBuilder(TopicName.of(Config.projectId, s"tasks"))
    .setCredentialsProvider(GOOGLE_CREDENTIALS_PROVIDER)
    .setBatchingSettings(psettings)
    .build()

  val subPublishers = TrieMap.empty[String, Publisher]

  for(i<-0 until Config.NUM_SUBSCRIBERS){
    val publisher = Publisher
      .newBuilder(TopicName.of(Config.projectId, s"subscriber-$i"))
      .setCredentialsProvider(GOOGLE_CREDENTIALS_PROVIDER)
      .setBatchingSettings(psettings)
      .build()

    subPublishers += i.toString -> publisher
  }

  val gcs = StorageOptions.newBuilder().setProjectId(Config.projectId)
    .setCredentials(GOOGLE_CREDENTIALS).build().getService()

  val receiver = new MessageReceiver {
    override def receiveMessage(message: PubsubMessage, consumer: AckReplyConsumer): Unit = {

      logger.info(s"${Console.GREEN_B}broker-$id received message: ${message}${Console.RESET}")

      val batch = Any.parseFrom(message.getData.toByteArray).unpack(PostBatch)

      Future.sequence(batch.posts.map{p => readFromStorage(p.id).map(p -> _)}).onComplete {
        case Success(posts) =>

          posts.foreach { case (post, data) =>
            post.subscribers.foreach { s =>

              endpoints.get(s) match {
                case Some(e) =>

                  e.publish(post.topic, io.vertx.core.buffer.Buffer.buffer(data), MqttQoS.valueOf(post.qos),
                    false, false)

                case None =>
              }
            }
          }

        case Failure(ex) => logger.info(ex.getMessage())
      }

      consumer.ack()
    }
  }

  val subscriber = Subscriber.newBuilder(subscriptionName, receiver)
    .setCredentialsProvider(GOOGLE_CREDENTIALS_PROVIDER)
    .setFlowControlSettings(flowControlSettings)
    .build()

  subscriber.startAsync.awaitRunning()

  def publishSubscription(topics: Seq[String], client: String): Future[Boolean] = {
    if(topics.isEmpty) return Future.successful(true)

    Future.sequence(topics.map{t => t -> computeSubscriptionTopic(t)}.groupBy(_._2).map { case (pid, topics) =>

      val cmd = Subscribe(UUID.randomUUID.toString, topics.map(_._1), brokerId, client)
      val data = ByteString.copyFrom(Any.pack(cmd).toByteArray)

      val pm = PubsubMessage.newBuilder().setData(data).build()
      val pr = Promise[Boolean]()

      logger.info(s"\n${Console.YELLOW_B}topic: ${topics.map(_._1)} pid: ${pid}${Console.RESET}\n")
      val publisher = subPublishers(pid.toString)

      publisher.publish(pm).addListener(() => pr.success(true), ec.asInstanceOf[Executor])

      pr.future
    }).map(!_.exists(_ == false))
  }

  val vertx = Vertx.vertx()

  val options = MqttServerOptions()
  options.setHost(host).setPort(port)
  val server = MqttServer.create(vertx, options)
  val endpoints = TrieMap.empty[String, MqttEndpoint]
  val subscriptions = TrieMap.empty[String, TrieMap[MqttEndpoint, MqttTopicSubscription]]

  def publishTask(task: Task): Future[Boolean] = {
    val data = ByteString.copyFrom(Any.pack(task).toByteArray)

    val pm = PubsubMessage.newBuilder().setData(data).build()
    val pr = Promise[Boolean]()

    taskPublisher.publish(pm).addListener(() => pr.success(true), ec.asInstanceOf[Executor])

    pr.future
  }

  def getRoot(topic: String): Future[Option[String]] = {
    storage.load(s"${topic}_subscribers").map(_.root).recover{case _ => None}
  }

  def writeToStorage(id: String, value: Array[Byte]): Future[Boolean] = {
    vertx.executeBlocking { () =>
      val blobId = BlobId.of(Config.bucketId, id)
      val blobInfo = BlobInfo.newBuilder(blobId).build()

      val info = gcs.create(blobInfo, value)
      logger.info(s"created bucket ${info}")
    }.map(_ => true).recover{case _ => false}
  }

  def readFromStorage(id: String): Future[Array[Byte]] = {

    logger.info(s"\ntrying to read ${id}...\n")

    vertx.executeBlocking { () =>
      val blobId = BlobId.of(Config.bucketId, id)
      gcs.get(blobId).getContent()
    }
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

      writeToStorage(mid, data).flatMap{ ok => getRoot(message.topicName())}.flatMap {
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
}