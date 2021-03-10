package services.scalable.pubsub

import org.apache.kafka.clients.admin.RecordsToDelete
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}
import org.apache.kafka.common.TopicPartition
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory

import java.io.FileInputStream
import collection.JavaConverters._
import java.util.Properties

class KafkaDeleteRecordsSpec extends AnyFlatSpec {

  "it " should "delete records successfully" in {

    val logger = LoggerFactory.getLogger(this.getClass)

   // val vertx = Vertx.vertx()
    val properties = new Properties()

    properties.load(new FileInputStream("kafka.properties"))

    //properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")

    //val client = KafkaAdminClient.create(vertx, properties)

    val client = AdminClient.create(properties)

    // Only works on linux machines (like in Confluent Cloud)
    val topics = Map[TopicPartition, RecordsToDelete](
      new TopicPartition("data", 0) -> RecordsToDelete.beforeOffset(90L)
    )

    client.deleteRecords(topics.asJava).all().get()

  }

}
