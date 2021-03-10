package services.scalable.pubsub

import akka.actor.typed.Behavior
import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter._
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
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
  final case class IndexChanged(indexes: Map[String, Option[String]]) extends Event

  def apply(name: String)(implicit cache: Cache[String, Bytes, Bytes],
                          storage: Storage[String, Bytes, Bytes]): Behavior[WorkerMessage] = Behaviors.setup { ctx =>

    val logger = ctx.log
    implicit val ec = ctx.executionContext

    val mediator = DistributedPubSub(ctx.system).mediator
    // subscribe to the topic named "content"
    mediator ! DistributedPubSubMediator.Subscribe(Topics.EVENTS, ctx.self.toClassic)

    ctx.log.info(s"${Console.YELLOW_B}Starting worker ${name}${Console.RESET}\n")
    ctx.system.receptionist ! Receptionist.Register(ServiceKey[Command](name), ctx.self)

    val subscriptionId = s"tasks-sub"
    val tasksSubscriptionName = ProjectSubscriptionName.of(Config.projectId, subscriptionId)

    val brokerPublishers = TrieMap.empty[String, Publisher]
    val indexes = TrieMap.empty[String, Index[String, Bytes, Bytes]]

    for(i<-0 until Broker.Config.NUM_BROKERS){
      val publisher = Publisher.newBuilder(TopicName.of(Config.projectId, s"broker-$i"))
        .setBatchingSettings(psettings)
        .build()

      brokerPublishers += i.toString -> publisher
    }

    val taskPublisher = Publisher.newBuilder(TopicName.of(Config.projectId, Topics.TASKS))
      .setBatchingSettings(psettings)
      .build()

    def publishBroker(post: Post, broker: String): Future[Boolean] = {
      val pm = PubsubMessage.newBuilder().setData(ByteString.copyFrom(Any.pack(post).toByteArray)).build()

      val pr = Promise[String]()

      val publisher = brokerPublishers(broker)

      ec.execute(() => {
        pr.success(publisher.publish(pm).get())
      })

      pr.future.map(_ => true)
    }

    def publishTask(m: Message, last: Option[String]): Future[Boolean] = {

      if(last.isEmpty) return Future.successful(true)

      // Next batch of subscribers...
      val task = Task(UUID.randomUUID.toString, Some(m), last.get)
      val pm = PubsubMessage.newBuilder().setData(ByteString.copyFrom(Any.pack(task).toByteArray)).build()

      val pr = Promise[String]()

      ec.execute(() => {
        pr.success(taskPublisher.publish(pm).get())
      })

      pr.future.map(_ => true)
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

    val queue = TrieMap.empty[String, (Task, AckReplyConsumer)]
    val timer = new java.util.Timer()

    class PublishTask extends TimerTask {
      override def run(): Unit = {

        val commands = queue.map(_._2._1).toSeq

        if(commands.isEmpty){
          timer.schedule(new PublishTask(), 100L)
          return
        }
        
        Future.sequence(commands.map{t => getSubscriptions(t.message.get.topic, t.lastBlock).map(t.message.get -> _)}).flatMap { subscriptions =>
          Future.sequence(subscriptions.map { case (m, (subscribers, last)) =>

            publishTask(m, last).flatMap { ok =>
              val brokers = subscribers.map{ case (s, _) => brokerClients(s) -> s}.groupBy(_._1).map{case (broker, list) =>
                broker -> list.map(_._2)
              }

              Future.sequence(brokers.map { case (b, list) =>
                val post = Post(UUID.randomUUID.toString, Some(m), list)
                publishBroker(post, b)
              })
            }
          })
        }.onComplete {
          case Success(ok) =>

            commands.foreach { t =>
              queue.remove(t.id).get._2.ack()
            }

            timer.schedule(new PublishTask(), 100L)
          case Failure(ex) => ex.printStackTrace()
        }

      }
    }

    timer.schedule(new PublishTask(), 100L)

    val tasksReceiver = new MessageReceiver {
      override def receiveMessage(message: PubsubMessage, consumer: AckReplyConsumer): Unit = {

        val t = Any.parseFrom(message.getData.toByteArray).unpack(Task)

        println(s"${Console.GREEN_B}worker $name received task ${t}${Console.RESET}\n")

        queue.put(t.id, t -> consumer)
      }
    }

    val tasksSubscriber = Subscriber.newBuilder(tasksSubscriptionName, tasksReceiver)
      .setFlowControlSettings(flowControlSettings)
      .build()

    tasksSubscriber.startAsync.awaitRunning()

    Behaviors.receiveMessage {
      case IndexChanged(roots) =>

        logger.info(s"\n${Console.CYAN_B}${name} RECEIVED PUBSUB MESSAGE: ${roots}${Console.RESET}\n")

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

        /*for {

        } yield {}*/

        Behaviors.stopped
      case _ => Behaviors.empty
    }
  }

}
