package services.scalable.pubsub

import akka.actor.typed.{Behavior, PostStop, SupervisorStrategy}
import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter._
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.google.api.gax.core.FixedCredentialsProvider
import com.google.cloud.pubsub.v1.{AckReplyConsumer, MessageReceiver, Publisher, Subscriber}
import com.google.protobuf.ByteString
import com.google.protobuf.any.Any
import com.google.pubsub.v1.{ProjectSubscriptionName, PubsubMessage, TopicName}
import io.vertx.scala.core.Vertx
import io.vertx.scala.kafka.client.consumer.{KafkaConsumer, KafkaConsumerRecords}
import io.vertx.scala.kafka.client.producer.{KafkaProducer, KafkaProducerRecord}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import services.scalable.index._
import services.scalable.index.impl.DefaultContext
import services.scalable.pubsub.grpc._

import java.util.{TimerTask, UUID}
import scala.collection.concurrent.TrieMap
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}

object Worker {
  sealed trait WorkerMessage

  sealed trait Command extends WorkerMessage
  sealed trait Event extends WorkerMessage

  final case object Stop extends Command
  //final case class IndexChanged(indexes: Map[String, Option[String]]) extends Event

  def apply(name: String, id: Int)(implicit cache: Cache[String, Bytes, Bytes],
                          storage: Storage[String, Bytes, Bytes]): Behavior[WorkerMessage] =

    Behaviors.supervise {

      Behaviors.setup[Worker.WorkerMessage] { ctx =>

        val logger = ctx.log
        implicit val ec = ctx.executionContext

        val mediator = DistributedPubSub(ctx.system).mediator
        // subscribe to the topic named "content"
        //mediator ! DistributedPubSubMediator.Subscribe(Topics.EVENTS, ctx.self.toClassic)

        ctx.log.info(s"${Console.YELLOW_B}Starting worker ${name}${Console.RESET}\n")
        ctx.system.receptionist ! Receptionist.Register(ServiceKey[Command](name), ctx.self)

        val vertx = Vertx.vertx(voptions)

        val cconfig = scala.collection.mutable.Map[String, String]()

        cconfig += (ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> Config.KAFKA_HOST)
        cconfig += (ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer")
        cconfig += (ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArrayDeserializer")

        cconfig += (ConsumerConfig.GROUP_ID_CONFIG -> "pubsub.workers")

        cconfig += (ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest")
        cconfig += (ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false")
        cconfig += (ConsumerConfig.MAX_POLL_RECORDS_CONFIG -> "1000")
        cconfig += (ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG -> "100")
        cconfig += (ConsumerConfig.FETCH_MIN_BYTES_CONFIG -> (1 * 1024 * 1024).toString)

        val consumer = KafkaConsumer.create[String, Array[Byte]](vertx, cconfig)

        val indexes = TrieMap.empty[String, Index[String, Bytes, Bytes]]

        /*for(i<-0 until Broker.Config.NUM_BROKERS){
          val publisher = Publisher
            .newBuilder(TopicName.of(Config.projectId, s"broker-$i"))
            .setCredentialsProvider(GOOGLE_CREDENTIALS_PROVIDER)
            .setBatchingSettings(psettings)
            .build()

          brokerPublishers += i.toString -> publisher
        }*/

        val pconfig = scala.collection.mutable.Map[String, String]()
        pconfig += (ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> Config.KAFKA_HOST)
        pconfig += (ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer")
        pconfig += (ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArraySerializer")
        pconfig += (ProducerConfig.ACKS_CONFIG -> "all")
        pconfig += (ProducerConfig.BATCH_SIZE_CONFIG -> (64 * 1024).toString)

        // use producer for interacting with Apache Kafka
        val producer = KafkaProducer.create[String, Array[Byte]](vertx, pconfig)

        def publishTasks(tasks: Seq[Task]): Future[Boolean] = {
          val pr = Promise[Boolean]()

          tasks.map { t =>
            val buf = Any.pack(t).toByteArray

            val record = KafkaProducerRecord.create[String, Array[Byte]](Topics.TASKS, t.id, buf)
            producer.write(record)
          }

          producer.flush(_ => pr.success(true)).exceptionHandler(ex => pr.failure(ex))

          pr.future
        }

        def getSubscriptions(topic: String, root: String, last: String = ""): Future[(Seq[(String, String)], Option[String])] = {
          val indexId = s"${topic}_subscribers"

          def getSubscriptions(index: Index[String, Bytes, Bytes]): Future[(Seq[(String, String)], Option[String])] = {
            index.next(if(last.isEmpty) None else Some(last)).map {
              case None => Seq.empty[(String, String)] -> None
              case Some(nxt) => nxt.inOrder()
                .map{case (k, v) => new String(k) -> new String(v)} -> Some(nxt.id)
            }
          }

          if(!indexes.isDefinedAt(root)){
            return storage.load(indexId).flatMap { ctx =>

              val index = new Index[String, Bytes, Bytes]()(ec, ctx)
              indexes.put(root, index)

              getSubscriptions(index)
            }.recover {
              case _ => Seq.empty[(String, String)] -> None
            }
          }

          getSubscriptions(indexes(root))
        }

        def post(posts: Seq[Post]): Future[Boolean] = {
          val pr = Promise[Boolean]()

          posts.map { p =>
            val buf = Any.pack(p).toByteArray

            val record = KafkaProducerRecord.create[String, Array[Byte]](p.kafkaTopic, p.id, buf)
            producer.write(record)
          }

          producer.flush(_ => pr.success(true)).exceptionHandler(ex => pr.failure(ex))

          pr.future
        }

        def commitAndResume(): Unit = {
          consumer.commitFuture().onComplete {
            case Success(ok) => consumer.resume()
            case Failure(ex) => throw ex
          }
        }

        def handler(recs: KafkaConsumerRecords[String, Array[Byte]]): Unit = {
          consumer.pause()

          val tasks = (0 until recs.size()).map { i =>
            val rec = recs.recordAt(i)
            Any.parseFrom(rec.value()).unpack(Task)
          }

          Future.sequence(tasks.map { t => getSubscriptions(t.topic, t.root, t.lastBlock).map {t -> _}}).flatMap { subs =>

            val list = subs.filterNot(_._2._1.isEmpty)

            var delivery = Seq.empty[Post]
            var nexts = Seq.empty[Task]

            list.foreach { case (m, (subs, last)) =>

              subs.groupBy(_._2).foreach { case (kafka_topic, subscribers) =>
                delivery = delivery :+ Post(m.id, kafka_topic, m.qos, subscribers.map(_._1))
              }

              if(last.isDefined){
                nexts = nexts :+ Task(m.id, m.topic, m.qos, m.root, last.get)
              }
            }

            /*var lasts = Seq.empty[(Task, String)]

            val brokers = list.map { case (m, (subs, last)) =>
              if (last.isDefined) {
                lasts = lasts :+ (m -> last.get)
              }

              m -> subs
            }

            Future.sequence(Seq(post(brokers), publishTasks(lasts.map { case (t, last) => Task(t.id, t.topic, t.qos, t.root, last)})))*/

            logger.info(s"${Console.BLUE_B}$name delivery => ${delivery} nexts ${nexts}${Console.RESET}\n")

            Future.sequence(Seq(
              post(delivery),
              publishTasks(nexts)
            ))
          }.onComplete {
            case Success(ok) => commitAndResume()
            case Failure(ex) => throw ex
          }
        }

        consumer.subscribeFuture(Topics.TASKS).onComplete {
          case Success(ok) =>

            consumer.handler(_ => {})
            consumer.batchHandler(handler)

          case Failure(ex) => throw ex
        }

        def closeAll[T](): Behavior[T] = {
          logger.info(s"${Console.RED_B}$name stopping at ${ctx.system.address}...${Console.RESET}")

          producer.close()
          consumer.close()
          vertx.close()

          Behaviors.same
        }

        Behaviors.receiveMessage[Worker.WorkerMessage] {
          case Stop =>

            Behaviors.stopped
          //case _ => Behaviors.empty
        }.receiveSignal {
          case (context, PostStop) =>

            closeAll()

            Behaviors.same

          case _ => Behaviors.same
        }
      }

    }.onFailure[Exception](SupervisorStrategy.restart)

}
