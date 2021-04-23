package services.scalable.pubsub

import io.vertx.scala.core.Vertx
import io.vertx.scala.kafka.admin.{KafkaAdminClient, NewTopic}
import io.vertx.scala.kafka.client.common.TopicPartition
import io.vertx.scala.kafka.client.consumer.KafkaConsumer
import org.apache.kafka.clients.admin.AdminClientConfig

import scala.concurrent.{ExecutionContext, Future}

object KafkaAdminHelper {

  val vertx = Vertx.vertx()

  val config = scala.collection.mutable.Map[String, String]()
  config += (AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> Config.KAFKA_HOST)

  val admin = KafkaAdminClient.create(vertx, config)

  def create(topics: Map[String, Int])(implicit ec: ExecutionContext): Future[Boolean] = {
    admin.createTopicsFuture(topics.map{case (name, p) =>
      NewTopic.apply(new io.vertx.kafka.admin.NewTopic(name, p, 1))
    }.toBuffer).map(_ => true)
  }

  def delete(topics: Seq[String])(implicit ec: ExecutionContext): Future[Boolean] = {
    admin.deleteTopicsFuture(topics.toBuffer).map(_ => true)
  }

  def topicsExists(topics: Seq[String])(implicit ec: ExecutionContext): Future[Seq[String]] = {
    admin.listTopicsFuture().map{list => topics.filter(list.contains(_))}
  }

  def configure(partition: TopicPartition, consumer: KafkaConsumer[String, Array[Byte]], offset: Long)(implicit ec: ExecutionContext): Future[Boolean] = {
    /*consumer.assignFuture(partition)
      .flatMap(_ => consumer.endOffsetsFuture(partition))
      .flatMap(last => consumer.seekFuture(partition, last))
      /*.flatMap(_ => YugabyteAdminHelper.insertOffsets(partition.getTopic, partition.getPartition, name))
      .flatMap(_ => YugabyteAdminHelper.getOffset(partition.getTopic, partition.getPartition, name))
      .flatMap(offset => consumer.seekFuture(partition, offset))*/
      .map(_ => true)*/

    consumer.assignFuture(partition)
      .flatMap(_ => consumer.seekFuture(partition, offset))
      .map(_ => true)
  }

}