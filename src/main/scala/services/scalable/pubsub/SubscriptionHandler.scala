package services.scalable.pubsub

import akka.actor.typed.SupervisorStrategy
import akka.actor.typed.{Behavior, PostStop, PreRestart}
import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.google.cloud.pubsub.v1.{AckReplyConsumer, MessageReceiver, Publisher, Subscriber}
import com.google.protobuf.ByteString
import com.google.protobuf.any.Any
import com.google.pubsub.v1.{ProjectSubscriptionName, PubsubMessage, TopicName}
import io.vertx.scala.core.Vertx
import io.vertx.scala.kafka.client.consumer.{KafkaConsumer, KafkaConsumerRecords}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import services.scalable.index._
import services.scalable.pubsub.grpc._

import java.util.{TimerTask, UUID}
import scala.collection.concurrent.TrieMap
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}

object SubscriptionHandler {

  sealed trait Command
  final case object Stop extends Command

  def apply(name: String, id: Int)(implicit cache: Cache[String, Bytes, Bytes],
                                   storage: Storage[String, Bytes, Bytes]): Behavior[Command] =
    Behaviors.supervise {

      Behaviors.setup[SubscriptionHandler.Command] { ctx =>

        val logger = ctx.log
        implicit val ec = ctx.executionContext

        ctx.log.info(s"${Console.YELLOW_B}Starting  ${name}${Console.RESET}\n")
        ctx.system.receptionist ! Receptionist.Register(ServiceKey[Command](name), ctx.self)

        val vertx = Vertx.vertx(voptions)

        // activate the extension
        val mediator = DistributedPubSub(ctx.system).mediator

        val cconfig = scala.collection.mutable.Map[String, String]()

        cconfig += (ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> Config.KAFKA_HOST)
        cconfig += (ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer")
        cconfig += (ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArrayDeserializer")

        cconfig += (ConsumerConfig.GROUP_ID_CONFIG -> s"pubsub.sub-${id}")

        cconfig += (ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest")
        cconfig += (ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false")
        cconfig += (ConsumerConfig.MAX_POLL_RECORDS_CONFIG -> "1000")
        cconfig += (ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG -> "100")
        cconfig += (ConsumerConfig.FETCH_MIN_BYTES_CONFIG -> (1 * 1024 * 1024).toString)

        val consumer = KafkaConsumer.create[String, Array[Byte]](vertx, cconfig)

        val indexes = TrieMap.empty[String, (Index[String, Bytes, Bytes], Context[String, Bytes, Bytes])]

        def addSubscribers(topic: String, subs: Seq[Subscribe]): Future[Boolean] = {

          if(subs.isEmpty) return Future.successful(true)

          val indexId = s"${topic}_subscribers"

          def addSubscribers(index: Index[String, Bytes, Bytes], ctx: Context[String, Bytes, Bytes]): Future[Boolean] = {

            logger.info(s"\ninserting into ${topic}: ${subs.map{s => s.subscriber -> s.externalTopic}}\n")

            index.insert(subs.map{s => s.subscriber.getBytes() -> s.externalTopic.getBytes()}, upsert = true)
              .flatMap { result =>

                logger.info(s"${Console.BLUE_B}INSERTED: ${result}${Console.RESET}")

                ctx.save()
              }
          }

          if(!indexes.isDefinedAt(indexId)) {
            return storage.loadOrCreate(indexId, Config.NUM_LEAF_ENTRIES, Config.NUM_META_ENTRIES).flatMap { ctx =>

              val index = new Index[String, Bytes, Bytes]()(ec, ctx)
              indexes.put(indexId, index -> ctx)

              addSubscribers(index, ctx)
            }
          }

          val (index, ctx) = indexes(indexId)

          addSubscribers(index, ctx)
        }

        def commitAndResume(): Unit = {
          consumer.commitFuture().onComplete {
            case Success(ok) => consumer.resume()
            case Failure(ex) => throw ex
          }
        }

        def handler(recs: KafkaConsumerRecords[String, Array[Byte]]): Unit = {
          consumer.pause()

          val commands = (0 until recs.size()).map { i =>
            val rec = recs.recordAt(i)
            Any.parseFrom(rec.value()).unpack(Subscribe)
          }

          var distinct = Map.empty[String, Map[String, Subscribe]]

          commands.foreach { s =>
            s.topics.foreach { t =>

              val opt = distinct.get(t)

              if(opt.isDefined){
                val subs = opt.get

                if(!subs.isDefinedAt(s.subscriber)){
                  distinct = distinct + (t -> (subs + (s.subscriber -> s)))
                }
              } else {
                distinct = distinct + (t -> Map(s.subscriber -> s))
              }
            }
          }

          println(s"\n${Console.MAGENTA_B}$name received subscriptions ${distinct}${Console.RESET}\n")

          Future.sequence(distinct.map{case (topic, list) => addSubscribers(topic, list.map(_._2).toSeq)}).onComplete {
            case Success(roots) => commitAndResume()
            case Failure(ex) => logger.error(ex.getMessage)
          }
        }

        consumer.subscribeFuture(s"sub-${id}").onComplete {
          case Success(ok) =>

            consumer.handler(_ => {})
            consumer.batchHandler(handler)

          case Failure(ex) => throw ex
        }

        def closeAll[T](): Behavior[T] = {
          logger.info(s"${Console.RED_B}$name stopping at ${ctx.system.address}...${Console.RESET}")

          consumer.close()
          vertx.close()

          Behaviors.same
        }

        Behaviors.receiveMessage[Command] {
          case Stop => Behaviors.stopped
          case _ => Behaviors.empty
        }.receiveSignal {
          case (context, PostStop) =>

            closeAll()

            Behaviors.same

          case (context, PreRestart) =>

            Behaviors.same

          case _ => Behaviors.same
        }
      }

    }.onFailure[Exception](SupervisorStrategy.restart)

}
