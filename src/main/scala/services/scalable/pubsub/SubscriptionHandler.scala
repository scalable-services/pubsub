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

        // activate the extension
        val mediator = DistributedPubSub(ctx.system).mediator

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

        val subscriptionId = s"subscriber-${id}-sub"
        val subscriptionName = ProjectSubscriptionName.of(Config.projectId, subscriptionId)

        val queue = TrieMap.empty[String, (Subscribe, AckReplyConsumer)]

        val timer = new java.util.Timer()

        class CommandTask extends TimerTask {
          override def run(): Unit = {

            val commands = queue.map(_._2._1).toSeq

            if(commands.isEmpty){
              timer.schedule(new CommandTask(), Config.SUBSCRIBER_BATCH_INTERVAL)
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

            println(s"\n${Console.MAGENTA_B}$name received subscriptions ${commands.map(_.subscriber)}${Console.RESET}\n")

            Future.sequence(distinct.map{case (topic, list) => addSubscribers(topic, list.map(_._2).toSeq)}).onComplete {
              case Success(roots) =>

                commands.foreach { s =>
                  queue.remove(s.id).get._2.ack()
                }

                timer.schedule(new CommandTask(), Config.SUBSCRIBER_BATCH_INTERVAL)

              case Failure(ex) => logger.error(ex.getMessage)
            }
          }
        }

        timer.schedule(new CommandTask(), Config.SUBSCRIBER_BATCH_INTERVAL)

        val receiver = new MessageReceiver {
          override def receiveMessage(message: PubsubMessage, consumer: AckReplyConsumer): Unit = {

            val s = Any.parseFrom(message.getData.toByteArray).unpack(Subscribe)

            println(s"\n${Console.MAGENTA_B}$name received message ${s.subscriber}${Console.RESET}\n")

            queue.put(s.id, s -> consumer)
          }
        }

        val subscriber = Subscriber
          .newBuilder(subscriptionName, receiver)
          .setCredentialsProvider(GOOGLE_CREDENTIALS_PROVIDER)
          .setFlowControlSettings(flowControlSettings)
          .build()

        /*if(subscriber.isRunning){
          subscriber.stopAsync().awaitTerminated()
        }*/

        subscriber.startAsync()//.awaitRunning()

        Behaviors.receiveMessage[Command] {
          case Stop =>

            /*ctx.log.info(s"${Console.RED_B}subscription handler $name is stopping${Console.RESET}\n")

            val pr = Promise[Boolean]()

            ec.execute(() => {
              timer.cancel()
              subscriber.stopAsync().awaitTerminated()
              pr.success(true)
            })*/

            Behaviors.stopped

          case _ => Behaviors.empty
        }.receiveSignal {
          case (context, PostStop) =>

            ctx.log.info(s"${Console.RED_B}subscription handler $name is stopping${Console.RESET}\n")

            val pr = Promise[Boolean]()

            ec.execute(() => {
              timer.cancel()
              subscriber.stopAsync().awaitTerminated()
              pr.success(true)
            })

            Behaviors.same

          case (context, PreRestart) =>

            Behaviors.same
        }
      }

    }.onFailure[Exception](SupervisorStrategy.restart)

}
