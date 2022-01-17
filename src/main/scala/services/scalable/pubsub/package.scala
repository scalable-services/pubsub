package services.scalable

import com.datastax.oss.driver.api.core.config.{DefaultDriverOption, DriverConfigLoader}
import com.google.common.hash.Hashing
import io.vertx.scala.core.VertxOptions

import java.time.Duration
import java.util.concurrent.CompletionStage
import scala.collection.concurrent.TrieMap
import scala.compat.java8.FutureConverters.toScala
import scala.concurrent.Future

package object pubsub {

  val brokerClients = TrieMap.empty[String, String]

  type TPartition = io.vertx.kafka.client.common.TopicPartition

  implicit def toScalaFuture[T](cs: CompletionStage[T]) = toScala[T](cs)

  val loader =
    DriverConfigLoader.programmaticBuilder()
      .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30))
      .withInt(DefaultDriverOption.CONNECTION_MAX_REQUESTS, 31768)
      .withInt(DefaultDriverOption.SESSION_LEAK_THRESHOLD, 1000)
      .withString(DefaultDriverOption.PROTOCOL_VERSION, "V4")
      .withString(DefaultDriverOption.RECONNECTION_POLICY_CLASS, "ExponentialReconnectionPolicy")
      .withDuration(DefaultDriverOption.RECONNECTION_BASE_DELAY, Duration.ofSeconds(1))
      .withDuration(DefaultDriverOption.RECONNECTION_MAX_DELAY, Duration.ofSeconds(10))
      /*.startProfile("slow")
      .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30))
      .endProfile()*/
      .build()

  val voptions = VertxOptions()
    .setClustered(false)
  /*.setWorkerPoolSize(4)
  .setBlockedThreadCheckInterval(10L)
  .setBlockedThreadCheckIntervalUnit(TimeUnit.SECONDS)*/

  object Config {
    val admin = true

    val NUM_LEAF_ENTRIES = 10
    val NUM_META_ENTRIES = 10

    val NUM_SUBSCRIBERS = 3
    val NUM_WORKERS = 3

    val KEYSPACE = "pubsub"

    val KAFKA_HOST = "localhost:9092"
    val ZOOKEEPER_HOST = "localhost:2181"

    //in ms
    val SUBSCRIBER_BATCH_INTERVAL = 10L
    val WORKER_BATCH_INTERVAL = 10L

    // All topics...
    var topics = Map(
      Topics.TASKS -> 3,
      Topics.SUBSCRIPTIONS -> 3
    )

    for(i<-0 until Config.NUM_SUBSCRIBERS){
      topics = topics + (s"broker-$i" -> 1)
    }

    for(i<-0 until Config.NUM_SUBSCRIBERS){
      topics = topics + (s"sub-$i" -> 1)
    }
  }

  object Topics {
    val SUBSCRIPTIONS = s"pubsub.subscriptions"
    val TASKS = s"pubsub.tasks"
  }

  val SUBSCRIBERS = (0 until Config.NUM_SUBSCRIBERS).map(s => s"subscription-$s")
  val WORKERS = (0 until Config.NUM_WORKERS).map(w => s"worker-$w")

  def computeSubscriptionTopic(topic: String): Int = {
    Hashing.murmur3_128().hashBytes(topic.getBytes()).asInt() % Config.NUM_SUBSCRIBERS
  }
}
