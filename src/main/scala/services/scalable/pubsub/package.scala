package services.scalable

import com.datastax.oss.driver.api.core.config.{DefaultDriverOption, DriverConfigLoader}
import com.google.api.gax.batching.{BatchingSettings, FlowControlSettings}
import com.google.api.gax.core.FixedCredentialsProvider
import com.google.auth.oauth2.GoogleCredentials
import com.google.common.collect.Lists
import com.google.common.hash.Hashing
import io.vertx.scala.core.VertxOptions

import java.io.FileInputStream
import java.time.Duration
import java.util.concurrent.{CompletionStage, TimeUnit}
import scala.collection.concurrent.TrieMap
import scala.compat.java8.FutureConverters.toScala
import scala.concurrent.Future

package object pubsub {

  val brokerClients = TrieMap.empty[String, String]

  implicit def asScalaFuture[T](cs: CompletionStage[T]): Future[T] = toScala(cs)

  val  requestBytesThreshold = 5000L // default : 1 byte
  val messageCountBatchSize = 100L // default : 1 message

  val publishDelayThreshold = org.threeten.bp.Duration.ofMillis(10)

  val psettings = BatchingSettings.newBuilder()
    .setElementCountThreshold(messageCountBatchSize)
    .setRequestByteThreshold(requestBytesThreshold)
    .setDelayThreshold(publishDelayThreshold)
    .build()

  val flowControlSettings =
    FlowControlSettings.newBuilder()
      // 1,000 outstanding messages. Must be >0. It controls the maximum number of messages
      // the subscriber receives before pausing the message stream.
      .setMaxOutstandingElementCount(100L)
      // 100 MiB. Must be >0. It controls the maximum size of messages the subscriber
      // receives before pausing the message stream.
      .setMaxOutstandingRequestBytes(100L * 1024L * 1024L)
      .build()

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
    val NUM_LEAF_ENTRIES = 10
    val NUM_META_ENTRIES = 10

    val NUM_SUBSCRIBERS = 3
    val NUM_WORKERS = 3

    val KEYSPACE = "pubsub"

    val projectId = "scalable-services"
    val projectRegion = "us-central1"

    val KAFKA_HOST = "localhost:9092"
    val ZOOKEEPER_HOST = "localhost:2181"
  }

  object Topics {
    val TASKS = "tasks"
    val SUBSCRIPTIONS = "subscriptions"
    val EVENTS = "worker-events"
  }

  val SUBSCRIBERS = (0 until Config.NUM_SUBSCRIBERS).map(s => s"subscription-$s")
  val WORKERS = (0 until Config.NUM_WORKERS).map(w => s"worker-$w")

  val GOOGLE_CREDENTIALS = GoogleCredentials.fromStream(new FileInputStream("google_cloud_credentials.json"))
    .createScoped(Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"))

  val GOOGLE_CREDENTIALS_PROVIDER = FixedCredentialsProvider.create(GOOGLE_CREDENTIALS)

  def computeSubscriptionTopic(topic: String): Int = {
    Hashing.murmur3_128().hashBytes(topic.getBytes()).asInt() % Config.NUM_SUBSCRIBERS
  }
}
