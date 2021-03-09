package services.scalable.pubsub

import akka.actor.typed.Behavior
import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.google.protobuf.any.Any
import io.vertx.scala.core.Vertx
import io.vertx.scala.kafka.client.consumer.{KafkaConsumer, KafkaConsumerRecords}
import io.vertx.scala.kafka.client.producer.{KafkaProducer, KafkaProducerRecord}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import services.scalable.index._
import services.scalable.pubsub.grpc._

import java.util.UUID
import scala.collection.concurrent.TrieMap
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}
import akka.actor.typed.scaladsl.adapter._
import services.scalable.index.impl.DefaultContext

object Worker {
  sealed trait WorkerMessage

  sealed trait Command extends WorkerMessage
  sealed trait Event extends WorkerMessage

  final case object Stop extends Command
  final case class IndexChanged(indexes: Map[String, Option[String]]) extends Event

  def apply(name: String)(implicit cache: Cache[String, Bytes, Bytes],
                          storage: Storage[String, Bytes, Bytes]): Behavior[WorkerMessage] = Behaviors.setup { ctx =>

    val logger = ctx.log
    implicit val ec = ctx.executionContext

    val mediator = DistributedPubSub(ctx.system).mediator
    // subscribe to the topic named "content"
    mediator ! DistributedPubSubMediator.Subscribe("events", ctx.self.toClassic)

    ctx.log.info(s"${Console.YELLOW_B}Starting worker ${name}${Console.RESET}\n")
    ctx.system.receptionist ! Receptionist.Register(ServiceKey[Command](name), ctx.self)

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
    cconfig += (ConsumerConfig.GROUP_ID_CONFIG -> "services.scalable.pubsub.workers")
    cconfig += (ConsumerConfig.MAX_POLL_RECORDS_CONFIG -> "10")
    cconfig += (ConsumerConfig.FETCH_MAX_BYTES_CONFIG -> (64 * 1024).toString)

    val consumer = KafkaConsumer.create[String, Array[Byte]](vertx, cconfig)

    val indexes = TrieMap.empty[String, Index[String, Bytes, Bytes]]

    def post(msgs: IndexedSeq[(Message, Seq[(String, String)])]): Future[Boolean] = {

      if(msgs.isEmpty) return Future.successful(true)

      val pr = Promise[Boolean]()
      val now = System.currentTimeMillis()

      msgs.foreach { case (m, subscribers) =>
        subscribers.groupBy(_._2).foreach { case (partition, subscribers) =>

          val post = Post(UUID.randomUUID.toString, Some(m), subscribers.map(_._1))
          val buf = Any.pack(post).toByteArray

          val record = KafkaProducerRecord.create[String, Array[Byte]](Broker.TOPIC, post.id, buf, now, partition.toInt)
          producer.write(record)

        }
      }

      producer.flush(_ => pr.success(true))

      pr.future.recover {case _ => false}
    }

    def publishTasks(tasks: Seq[Task]): Future[Boolean] = {
      if(tasks.isEmpty) return Future.successful(true)

      val pr = Promise[Boolean]()

      tasks.foreach { t =>
        val buf = Any.pack(t).toByteArray
        val record = KafkaProducerRecord.create[String, Array[Byte]](Topics.TASKS, t.id, buf)
        producer.write(record)
      }

      producer.flush(_ => pr.success(true))

      pr.future.recover {case _ => false}
    }

    def getSubscriptions(topic: String, last: String = ""): Future[(Seq[(String, String)], Option[String])] = {
      val indexId = s"${topic}_subscribers"

      def getSubscriptions(index: Index[String, Bytes, Bytes]): Future[(Seq[(String, String)], Option[String])] = {
        index.next(if(last.isEmpty) None else Some(last)).map {
          case None => Seq.empty[(String, String)] -> None
          case Some(nxt) => nxt.inOrder()
            .map{case (k, v) => new String(k) -> new String(v)} -> Some(nxt.id)
        }
      }

      if(!indexes.isDefinedAt(indexId)){
        return storage.load(indexId).flatMap { ctx =>

          val index = new Index[String, Bytes, Bytes]()(ec, ctx)
          indexes.put(indexId, index)

          getSubscriptions(index)
        }
      }

      getSubscriptions(indexes(indexId))
    }

    def handler(evts: KafkaConsumerRecords[String, Array[Byte]]): Unit = {
      consumer.pause()

      val tasks = (0 until evts.size()).map { i =>
        val rec = evts.recordAt(i)
        Any.parseFrom(rec.value()).unpack(Task)
      }

      println(s"${Console.GREEN_B}tasks: ${tasks}${Console.RESET}")

      Future.sequence(tasks.map{t => getSubscriptions(t.message.get.topic, t.lastBlock).map{t.message.get -> _}}).flatMap { subs =>

        val list = subs.filterNot(_._2._1.isEmpty)

        var lasts = Seq.empty[(Message, String)]

        val brokers = list.map { case (m, (subs, last)) =>
          if(last.isDefined){
            lasts = lasts :+ (m -> last.get)
          }

          m -> subs
        }

        post(brokers).flatMap(_ => publishTasks(lasts.map { case (m, last) =>
          Task(UUID.randomUUID.toString, Some(m), last)
        }))}.flatMap(_ => consumer.commitFuture()).onComplete {
        case Success(ok) => consumer.resume()
        case Failure(ex) => ex.printStackTrace()
      }
    }

    consumer.handler(_ => {})
    consumer.batchHandler(handler)
    consumer.subscribe(Topics.TASKS)

    Behaviors.receiveMessage {
      case IndexChanged(roots) =>

        logger.info(s"\n${Console.MAGENTA_B}${name} RECEIVED PUBSUB MESSAGE: ${roots}${Console.RESET}\n")

        roots.foreach { case (idx, root) =>
          if(indexes.isDefinedAt(idx)){
            val i = indexes(idx).asInstanceOf[DefaultContext]

            val ctx = new DefaultContext(i.indexId, root, i.NUM_LEAF_ENTRIES,
              i.NUM_META_ENTRIES)
            val index = new Index[String, Bytes, Bytes]()(ec, ctx)

            indexes.update(idx, index)
          }
        }

        Behaviors.same

      case Stop =>

        ctx.log.info(s"${Console.RED_B}WORKER IS STOPPING: ${name}${Console.RESET}\n")

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
