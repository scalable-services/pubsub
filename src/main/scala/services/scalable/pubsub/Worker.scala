package services.scalable.pubsub

import akka.actor.typed.{Behavior, PostStop}
import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter._
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.google.api.gax.core.FixedCredentialsProvider
import com.google.cloud.pubsub.v1.{AckReplyConsumer, MessageReceiver, Publisher, Subscriber}
import com.google.protobuf.ByteString
import com.google.protobuf.any.Any
import com.google.pubsub.v1.{ProjectSubscriptionName, PubsubMessage, TopicName}
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
                          storage: Storage[String, Bytes, Bytes]): Behavior[WorkerMessage] = Behaviors.setup { ctx =>

    val logger = ctx.log
    implicit val ec = ctx.executionContext

    val mediator = DistributedPubSub(ctx.system).mediator
    // subscribe to the topic named "content"
    mediator ! DistributedPubSubMediator.Subscribe(Topics.EVENTS, ctx.self.toClassic)

    ctx.log.info(s"${Console.YELLOW_B}Starting worker ${name}${Console.RESET}\n")
    ctx.system.receptionist ! Receptionist.Register(ServiceKey[Command](name), ctx.self)

    val tasksSubscriptionName = ProjectSubscriptionName.of(Config.projectId, s"tasks-sub")

    val brokerPublishers = TrieMap.empty[String, Publisher]
    val indexes = TrieMap.empty[String, Index[String, Bytes, Bytes]]

    /*for(i<-0 until Broker.Config.NUM_BROKERS){
      val publisher = Publisher
        .newBuilder(TopicName.of(Config.projectId, s"broker-$i"))
        .setCredentialsProvider(GOOGLE_CREDENTIALS_PROVIDER)
        .setBatchingSettings(psettings)
        .build()

      brokerPublishers += i.toString -> publisher
    }*/

    val taskPublisher = Publisher
      .newBuilder(TopicName.of(Config.projectId, s"tasks"))
      .setCredentialsProvider(GOOGLE_CREDENTIALS_PROVIDER)
      .setBatchingSettings(psettings)
      .build()

    def publishTasks(tasks: Seq[Task]): Future[Boolean] = {
      if(tasks.isEmpty) return Future.successful(true)

      Future.sequence(tasks.map { t =>

        val buf = Any.pack(t).toByteArray
        val pr = Promise[Boolean]()

        taskPublisher.publish(PubsubMessage.newBuilder().setData(ByteString.copyFrom(buf)).build()).addListener(() => {
          pr.success(true)
        }, ec)

        pr.future
      }).map(_ => true)
    }

    def createOrGetPublisher(topic: String): Publisher = {
      brokerPublishers.get(topic) match {
        case None =>

          val publisher = Publisher
            .newBuilder(TopicName.of(Config.projectId, topic))
            .setCredentialsProvider(GOOGLE_CREDENTIALS_PROVIDER)
            .setBatchingSettings(psettings)
            .build()

          brokerPublishers += topic -> publisher

          publisher

        case Some(p) => p
      }
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

    val queue = TrieMap.empty[String, (Task, AckReplyConsumer)]
    val timer = new java.util.Timer()

    def post(msgs: Seq[(Task, Seq[(String, String)])]): Future[Boolean] = {
      if(msgs.isEmpty) return Future.successful(true)

      val delivery = TrieMap.empty[String, Seq[(Task, Seq[String])]]

      msgs.foreach { case (t, subscribers) =>
        subscribers.groupBy(_._2).foreach { case (broker, clients_topics) =>
          delivery.get(broker) match {
            case None =>

              delivery.put(broker, Seq(t -> clients_topics.map(_._1)))

            case Some(list) => delivery.put(broker, list :+ (t -> clients_topics.map(_._1)))
          }
        }
      }

      Future.sequence(delivery.map { case (broker, list) =>
        PostBatch(UUID.randomUUID.toString, broker, list.map{ case (t, clients) =>
          logger.info(s"\n\n${Console.BLUE_B}subscribers: ${clients}\n\n${Console.RESET}")

          Post(UUID.randomUUID.toString, t.message, clients)
        })
      }.map { batch =>

        val buf = Any.pack(batch).toByteArray

        val pr = Promise[Boolean]()
        val broker = createOrGetPublisher(batch.topic)

        broker.publish(PubsubMessage.newBuilder().setData(ByteString.copyFrom(buf)).build())
          .addListener(() => pr.success(true), ec)

        pr.future
      }).map(_ => true)
    }

    class PublishTask extends TimerTask {
      override def run(): Unit = {

        val tasks = queue.map(_._2._1).toSeq

        if (tasks.isEmpty) {
          timer.schedule(new PublishTask(), 10L)
          return
        }

        Future.sequence(tasks.map { t => getSubscriptions(t.message.get.topic, t.root, t.lastBlock).map {t -> _}
        }).flatMap { subs =>

          val list = subs.filterNot(_._2._1.isEmpty)

          var lasts = Seq.empty[(Task, String)]

          val brokers = list.map { case (m, (subs, last)) =>
            if (last.isDefined) {
              lasts = lasts :+ (m -> last.get)
            }

            m -> subs
          }

          Future.sequence(Seq(post(brokers), publishTasks(lasts.map { case (t, last) =>
            Task(UUID.randomUUID.toString, t.message, t.root, last)
          })))

        }.onComplete {
          case Success(ok) =>

            tasks.foreach { t =>
              queue.remove(t.id).get._2.ack()
            }

            timer.schedule(new PublishTask(), 10L)

          case Failure(ex) => ex.printStackTrace()
        }
      }
    }

    timer.schedule(new PublishTask(), 10L)

    val tasksReceiver = new MessageReceiver {
      override def receiveMessage(message: PubsubMessage, consumer: AckReplyConsumer): Unit = {

        val t = Any.parseFrom(message.getData.toByteArray).unpack(Task)

        println(s"${Console.GREEN_B}worker $name received task ${t}${Console.RESET}\n")

        queue.put(t.id, t -> consumer)
      }
    }

    val tasksSubscriber = Subscriber
      .newBuilder(tasksSubscriptionName, tasksReceiver)
      .setCredentialsProvider(GOOGLE_CREDENTIALS_PROVIDER)
      .setFlowControlSettings(flowControlSettings)
      .build()

    tasksSubscriber.startAsync()//.awaitRunning()

    Behaviors.receiveMessage[WorkerMessage] {
      case Stop =>

        /*ctx.log.info(s"${Console.RED_B}WORKER IS STOPPING: ${name}${Console.RESET}\n")

        ec.execute(() => {
          timer.cancel()
          taskPublisher.shutdown()
          brokerPublishers.foreach(_._2.shutdown())
          tasksSubscriber.stopAsync().awaitTerminated()
        })*/

        Behaviors.stopped
      //case _ => Behaviors.empty
    }.receiveSignal {
      case (context, PostStop) =>

        ctx.log.info(s"${Console.RED_B}WORKER IS STOPPING: ${name}${Console.RESET}\n")

        ec.execute(() => {
          timer.cancel()
          taskPublisher.shutdown()
          brokerPublishers.foreach(_._2.shutdown())
          tasksSubscriber.stopAsync().awaitTerminated()
        })

        Behaviors.same
    }
  }

}
