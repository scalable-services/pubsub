package services.scalable.pubsub

import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorSystem, Behavior}
import akka.cluster.sharding.typed.ShardedDaemonProcessSettings
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityTypeKey, ShardedDaemonProcess}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import services.scalable.index.impl.{CassandraStorage, DefaultCache}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Await
import scala.concurrent.duration._
import services.scalable.index._

object Main {

  implicit val cache: Cache[String, Bytes, Bytes] = new DefaultCache()
  implicit val storage: Storage[String, Bytes, Bytes] = new CassandraStorage("indexes", truncate = true)

  val TypeKey = EntityTypeKey[Greeter.Command]("Greeter")
  //val WorkerServiceKey = ServiceKey[Command]("WorkerService")

  implicit val timeout = Timeout(5 seconds)

  object RootBehavior {
    def apply(port: Int)(implicit cache: Cache[String, Bytes, Bytes],
                         storage: Storage[String, Bytes, Bytes]): Behavior[Nothing] = Behaviors.setup[Nothing] { context =>
      // Create an actor that handles cluster domain events
      //context.spawn(ClusterListener(), "ClusterListener")

      val sharding = ClusterSharding(context.system)
      implicit val scheduler = context.system.scheduler

      /*val ref = sharding.init(
        Entity(TypeKey)(createBehavior = entityContext =>
          Greeter(entityContext.entityId, entityContext))
      )*/

      new GreeterServer(context.system, sharding).run(port + 1000)

      val daemon =  ShardedDaemonProcess(context.system)

      daemon.init("subscribers", SUBSCRIBERS.length, id => Subscriber(SUBSCRIBERS(id), id), Subscriber.Stop)
      daemon.init("workers", WORKERS.length, id => Worker(WORKERS(id)), Worker.Stop)

      Behaviors.empty
    }
  }

  def main(args: Array[String]): Unit = {

    val ports =
      if (args.isEmpty)
        Seq(2551, 2552)
      else
        args.toSeq.map(_.toInt)

    val topics = Map(
      Topics.SUBSCRIPTIONS -> 1,
      Topics.TASKS -> 3,
      Broker.TOPIC -> Broker.Config.NUM_BROKERS
    )

    Await.result(KafkaAdminHelper.create(topics).map { _ =>
      println(s"${Console.BLUE_B}created topics: ${topics}...${Console.RESET}\n\n")
    }, Duration.Inf)

    val basePort = 3000

    for(i<-0 until Broker.Config.NUM_BROKERS){
      val broker = new Broker(i.toString, "localhost", basePort + i)
    }

    ports.foreach(startup)
  }

  def startup(port: Int): Unit = {
    // Override the configuration of the port
    val config = ConfigFactory.parseString(s"""
      akka.remote.artery.canonical.port=$port
      """).withFallback(ConfigFactory.load())

    // Create an Akka system
    ActorSystem[Nothing](RootBehavior(port), "PubSub", config)
  }

}
