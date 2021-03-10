package services.scalable.pubsub

import com.datastax.oss.driver.api.core.CqlSession
import com.google.api.gax.core.FixedCredentialsProvider
import com.google.cloud.pubsub.v1.{AckReplyConsumer, MessageReceiver, Publisher, Subscriber}
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
import services.scalable.pubsub.grpc._

import java.nio.charset.Charset
import java.util.UUID
import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

class Broker(val id: String, val host: String, val port: Int)(implicit val ec: ExecutionContext) {

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
    .newBuilder(TopicName.of(Config.projectId, Topics.TASKS))
    .setCredentialsProvider(FixedCredentialsProvider.create(GOOGLE_CREDENTIALS))
    .setBatchingSettings(psettings)
    .build()

  val subPublishers = TrieMap.empty[String, Publisher]

  for(i<-0 until Config.NUM_SUBSCRIBERS){
    val publisher = Publisher
      .newBuilder(TopicName.of(Config.projectId, s"subscriptions-$i"))
      .setCredentialsProvider(GOOGLE_CREDENTIALS_PROVIDER)
      .setBatchingSettings(psettings)
      .build()

    subPublishers += i.toString -> publisher
  }

  val receiver = new MessageReceiver {
    override def receiveMessage(message: PubsubMessage, consumer: AckReplyConsumer): Unit = {

      logger.info(s"${Console.GREEN_B}broker-$id received message: ${message}${Console.RESET}")

      val batch = Any.parseFrom(message.getData.toByteArray).unpack(PostBatch)

      batch.posts.foreach { post =>
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

      consumer.ack()
    }
  }

  val subscriber = Subscriber.newBuilder(subscriptionName, receiver)
    .setFlowControlSettings(flowControlSettings)
    .build()

  subscriber.startAsync.awaitRunning()

  def insertSubscription(topics: Seq[String], client: String): Future[Boolean] = {
    Future.sequence(topics.map{t => t -> computeSubscriptionTopic(t)}.map{ case (topic, pid) => {

      val cmd = Subscribe(UUID.randomUUID.toString, Seq(topic), id, client)
      val data = ByteString.copyFrom(Any.pack(cmd).toByteArray)

      val pm = PubsubMessage.newBuilder().setData(data).build()
      val pr = Promise[Boolean]()

      val publisher = subPublishers(pid)

      publisher.publish(pm).addListener(() => pr.success(true), ec)

      pr.future
    }}).map(_ => true)
  }

  val vertx = Vertx.vertx()

  val options = MqttServerOptions()
  options.setHost(host).setPort(port)
  val server = MqttServer.create(vertx, options)
  val endpoints = TrieMap.empty[String, MqttEndpoint]
  val subscriptions = TrieMap.empty[String, TrieMap[MqttEndpoint, MqttTopicSubscription]]

  def endpointHandler(endpoint: MqttEndpoint): Unit = {

    val client = endpoint.clientIdentifier()

    endpoints.put(client, endpoint)
    brokerClients.put(client, id)

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

      if(!topics.isEmpty){
        insertSubscription(topics, client).onComplete {
          case Success(ok) => println(s"subscriptions ${topics} for broker ${id} inserted!!!\n")
          case Failure(ex) => ex.printStackTrace()
        }
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

      val m = Message(UUID.randomUUID.toString, message.topicName(),
        ByteString.copyFrom(message.payload().getBytes))

      val t = Task(UUID.randomUUID.toString, Some(m))
      val data = ByteString.copyFrom(Any.pack(t).toByteArray)

      val pm = PubsubMessage.newBuilder().setData(data).build()
      val pr = Promise[String]()

      ec.execute(() => pr.success(taskPublisher.publish(pm).get()))

      pr.future.onComplete {
        case Success(mid) =>

          println(s"${Console.GREEN_B}successfully published message with id ${mid}${Console.RESET}\n")

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