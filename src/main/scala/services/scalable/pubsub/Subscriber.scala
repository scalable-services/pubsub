package services.scalable.pubsub

import akka.actor.typed.Behavior
import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.google.protobuf.any.Any
import io.vertx.scala.core.Vertx
import io.vertx.scala.kafka.client.common.TopicPartition
import io.vertx.scala.kafka.client.consumer.{KafkaConsumer, KafkaConsumerRecords}
import io.vertx.scala.kafka.client.producer.KafkaProducer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import services.scalable.index._
import services.scalable.pubsub.grpc._

import scala.collection.concurrent.TrieMap
import scala.concurrent.Future
import scala.util.{Failure, Success}

object Subscriber {

  sealed trait Command
  final case object Stop extends Command

  def apply(name: String, id: Int)(implicit cache: Cache[String, Bytes, Bytes],
                                   storage: Storage[String, Bytes, Bytes]): Behavior[Command] = Behaviors.setup { ctx =>

    val logger = ctx.log
    implicit val ec = ctx.executionContext

    ctx.log.info(s"${Console.YELLOW_B}Starting subscriber ${name}${Console.RESET}\n")
    ctx.system.receptionist ! Receptionist.Register(ServiceKey[Command](name), ctx.self)

    import akka.cluster.pubsub.DistributedPubSubMediator.Publish
    // activate the extension
    val mediator = DistributedPubSub(ctx.system).mediator

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
    //cconfig += (ConsumerConfig.GROUP_ID_CONFIG -> "services.scalable.pubsub.subscribers")
    cconfig += (ConsumerConfig.MAX_POLL_RECORDS_CONFIG -> "10")
    cconfig += (ConsumerConfig.FETCH_MAX_BYTES_CONFIG -> (64 * 1024).toString)

    val consumer = KafkaConsumer.create[String, Array[Byte]](vertx, cconfig)
    val topicPartition = TopicPartition(new TPartition(Topics.SUBSCRIPTIONS, id))

    consumer.assign(topicPartition)

    val indexes = TrieMap.empty[String, (Index[String, Bytes, Bytes], Context[String, Bytes, Bytes])]

    def addSubscribers(topic: String, subs: Seq[Subscribe]): Future[Boolean] = {
      if(subs.isEmpty) return Future.successful(true)

      val indexId = s"${topic}_subscribers"

      def addSubscribers(index: Index[String, Bytes, Bytes], ctx: Context[String, Bytes, Bytes]): Future[Boolean] = {

        logger.info(s"\ninserting into ${topic}: ${subs.map{s => s.subscriber -> s.kafkaPartition}}\n")

        index.insert(subs.map{s => s.subscriber.getBytes() -> s.kafkaPartition.getBytes()}, upsert = true)
          .flatMap { result =>

            logger.info(s"${Console.BLUE_B}INSERTED: ${result}${Console.RESET}")

            ctx.save()
          }
      }

      if(!indexes.isDefinedAt(indexId)) {
        return storage.loadOrCreate(indexId, 5, 5).flatMap{ ctx =>

          val index = new Index[String, Bytes, Bytes]()(ec, ctx)
          indexes.put(indexId, index -> ctx)

          addSubscribers(index, ctx)
        }
      }

      val (index, ctx) = indexes(indexId)

      addSubscribers(index, ctx)
    }

    def handler(evts: KafkaConsumerRecords[String, Array[Byte]]): Unit = {
      consumer.pause()

      val commands = (0 until evts.size()).map { i =>
        val rec = evts.recordAt(i)
        Any.parseFrom(rec.value()).unpack(Subscribe)
      }

      logger.info(s"${Console.MAGENTA_B}${name} received commands ${commands}${Console.RESET}\n")

      var topics = commands.groupBy(_.topic)

      Future.sequence(topics.map { case (topic, subs) =>

        val distinct = TrieMap.empty[String, Subscribe]

        subs.foreach { s =>
          if(!distinct.isDefinedAt(s.subscriber)){
            distinct += s.subscriber -> s
          }
        }

        addSubscribers(topic, distinct.map(_._2).toSeq)
      })/*.flatMap {_ => consumer.commitFuture()}*/.onComplete {
        case Success(ok) =>

          val changes = topics.map { case (t, _) =>
            val name = s"${t}_subscribers"
            val (_, ctx) = indexes(name)

            name -> ctx.root
          }

          if(!changes.isEmpty){
            mediator ! DistributedPubSubMediator.Publish(Topics.EVENTS,
              Worker.IndexChanged(changes))
          }

          consumer.resume()
        case Failure(ex) => ex.printStackTrace()
      }
    }

    consumer.handler(_ => {})
    consumer.batchHandler(handler)

    Behaviors.receiveMessage {
      case Stop =>

        ctx.log.info(s"${Console.RED_B}subscriber is stopping: ${name}${Console.RESET}\n")

        for {
          _ <- producer.closeFuture()
          _ <- consumer.closeFuture()
          _ <- vertx.closeFuture()
        } yield {}

        Behaviors.stopped

      case _ => Behaviors.empty
    }
  }

}
