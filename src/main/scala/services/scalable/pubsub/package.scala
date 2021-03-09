package services.scalable

import com.datastax.oss.driver.api.core.config.{DefaultDriverOption, DriverConfigLoader}
import io.vertx.scala.core.VertxOptions

import java.time.Duration
import java.util.concurrent.{CompletionStage, TimeUnit}
import scala.compat.java8.FutureConverters.toScala
import scala.concurrent.Future

package object pubsub {

  implicit def asScalaFuture[T](cs: CompletionStage[T]): Future[T] = toScala(cs)

  val loader =
    DriverConfigLoader.programmaticBuilder()
      .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(5))
      .withInt(DefaultDriverOption.CONNECTION_MAX_REQUESTS, 31768)
      /*.startProfile("slow")
      .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30))
      .endProfile()*/
      .build()

  val voptions = VertxOptions()
    .setMaxWorkerExecuteTimeUnit(TimeUnit.SECONDS)
    .setMaxWorkerExecuteTime(10L)
    .setMaxEventLoopExecuteTimeUnit(TimeUnit.SECONDS)
    .setMaxEventLoopExecuteTime(60L)
    .setBlockedThreadCheckIntervalUnit(TimeUnit.SECONDS)
    .setBlockedThreadCheckInterval(65L)

  type TPartition = io.vertx.kafka.client.common.TopicPartition

  object Config {
    val NUM_SUBSCRIBERS = 1
    val NUM_WORKERS = 3

    val KEYSPACE = "pubsub"

    val KAFKA_HOST = "localhost:9092"
    val ZOOKEEPER_HOST = "localhost:2181"
  }

  object Topics {
    val SUBSCRIPTIONS = "services.scalable.pubsub.subscriptions"
    val TASKS = "services.scalable.pubsub.tasks"

    val EVENTS = "services.scalable.pubsub.events"
  }

  val SUBSCRIBERS = (0 until Config.NUM_SUBSCRIBERS).map(s => s"subscriber-$s")
  val WORKERS = (0 until Config.NUM_WORKERS).map(w => s"worker-$w")

}
