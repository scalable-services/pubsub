package services.scalable.pubsub

import akka.actor.typed.Behavior
import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.google.api.gax.core.FixedCredentialsProvider
import com.google.cloud.pubsub.v1.{AckReplyConsumer, MessageReceiver, Publisher, Subscriber, SubscriptionAdminClient}
import com.google.protobuf.ByteString
import com.google.protobuf.any.Any
import com.google.pubsub.v1.{ProjectSubscriptionName, PubsubMessage, TopicName}
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
                                   storage: Storage[String, Bytes, Bytes]): Behavior[Command] = Behaviors.setup { ctx =>

    val logger = ctx.log
    implicit val ec = ctx.executionContext

    ctx.log.info(s"${Console.YELLOW_B}Starting subscription handler ${name}${Console.RESET}\n")
    ctx.system.receptionist ! Receptionist.Register(ServiceKey[Command](name), ctx.self)
    // activate the extension
    val mediator = DistributedPubSub(ctx.system).mediator

    val indexes = TrieMap.empty[String, (Index[String, Bytes, Bytes], Context[String, Bytes, Bytes])]

    def addSubscribers(topic: String, subs: Seq[Subscribe]): Future[Boolean] = {

      if(subs.isEmpty) return Future.successful(true)

      val indexId = s"${topic}_subscribers"

      def addSubscribers(index: Index[String, Bytes, Bytes], ctx: Context[String, Bytes, Bytes]): Future[Boolean] = {

        logger.info(s"\ninserting into ${topic}: ${subs.map{s => s.subscriber -> s.brokerId}}\n")

        index.insert(subs.map{s => s.subscriber.getBytes() -> s.brokerId.getBytes()}, upsert = true)
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

    val subscriptionId = s"subscriptions-sub"
    val subscriptionName = ProjectSubscriptionName.of(Config.projectId, subscriptionId)

    val queue = TrieMap.empty[String, (Subscribe, AckReplyConsumer)]

    val timer = new java.util.Timer()

    /*val eventsPublisher = Publisher.newBuilder(TopicName.of(Config.projectId, Topics.EVENTS))
      .setBatchingSettings(psettings)
      .setEnableMessageOrdering(true)
      .build()*/

    class CommandTask extends TimerTask {
      override def run(): Unit = {

        val commands = queue.map(_._2._1).toSeq

        if(commands.isEmpty){
          timer.schedule(new CommandTask(), 10L)
          return
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
        
        Future.sequence(distinct.map { case (topic, subs) =>
          addSubscribers(topic, subs.map(_._2).toSeq)
        }).onComplete {
          case Success(roots) =>

            commands.foreach { s =>
              queue.remove(s.id).get._2.ack()
            }

            if(!roots.isEmpty){
              val evt = Worker.IndexChanged(distinct.map{case (topic, _) =>
                val topic_name = s"${topic}_subscribers"

                topic_name -> indexes(topic_name)._2.root})
              mediator ! DistributedPubSubMediator.Publish(Topics.EVENTS, evt)
            }

            timer.schedule(new CommandTask(), 10L)

          case Failure(ex) => ex.printStackTrace()
        }
      }
    }

    timer.schedule(new CommandTask(), 10L)

    val receiver = new MessageReceiver {
      override def receiveMessage(message: PubsubMessage, consumer: AckReplyConsumer): Unit = {

        val s = Any.parseFrom(message.getData.toByteArray).unpack(Subscribe)

        println(s"${Console.MAGENTA_B}commander received message ${s}${Console.RESET}\n")

        /*addSubscribers(s.topic, Seq(s)).onComplete {
          case Success(ok) => consumer.ack()
          case Failure(ex) => ex.printStackTrace()
        }*/

        queue += s.id -> (s, consumer)
        // consumer.ack()
      }
    }

    val subscriber = Subscriber
      .newBuilder(subscriptionName, receiver)
      .setCredentialsProvider(GOOGLE_CREDENTIALS_PROVIDER)
      .setFlowControlSettings(flowControlSettings)
      .build()

    subscriber.startAsync().awaitRunning()

    Behaviors.receiveMessage {
      case Stop =>

        ctx.log.info(s"${Console.RED_B}subscription handler $name is stopping${Console.RESET}\n")

        /*for {

        } yield {}*/

        Behaviors.stopped

      case _ => Behaviors.empty
    }
  }

}
