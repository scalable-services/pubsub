package services.scalable.pubsub

import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorSystem, Behavior}
import akka.cluster.sharding.typed.ShardedDaemonProcessSettings
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityTypeKey, ShardedDaemonProcess}
import akka.util.Timeout
import com.datastax.oss.driver.api.core.CqlSession
import com.google.auth.oauth2.GoogleCredentials
import com.google.common.collect.Lists
import com.typesafe.config.ConfigFactory
import services.scalable.index.impl.{CassandraStorage, DefaultCache}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Await
import scala.concurrent.duration._
import services.scalable.index._

import java.io.FileInputStream

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

      //new GreeterServer(context.system, sharding).run(port + 1000)

      val daemon =  ShardedDaemonProcess(context.system)

      daemon.init("subscribers", SUBSCRIBERS.length, id => SubscriptionHandler(SUBSCRIBERS(id), id), SubscriptionHandler.Stop)
      daemon.init("workers", WORKERS.length, id => Worker(WORKERS(id), id), Worker.Stop)

      Behaviors.empty
    }
  }

  def main(args: Array[String]): Unit = {

    val session = CqlSession
      .builder()
      .withConfigLoader(loader)
      .withKeyspace(Config.KEYSPACE)
      .build()

    session.execute(s"TRUNCATE TABLE messages;")

    val ports =
      if (args.isEmpty)
        Seq(2551, 2552, 2553)
      else
        args.toSeq.map(_.toInt)

    val basePort = 3000

    for(i<-0 until Config.NUM_SUBSCRIBERS){
      val broker = new Broker(i.toString, "localhost", basePort + i)
    }

    /*PubSubHelper.createTopics(Config.topics)
    PubSubHelper.createSubscriptions(Config.subscriptions)*/

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
