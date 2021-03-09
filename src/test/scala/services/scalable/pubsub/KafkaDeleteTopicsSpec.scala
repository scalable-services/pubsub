package services.scalable.pubsub

import org.apache.kafka.clients.admin.Admin
import org.scalatest.flatspec.AnyFlatSpec

import java.util.Properties
import scala.collection.JavaConverters._

class KafkaDeleteTopicsSpec extends AnyFlatSpec {

  "it " should "delete kafka topics" in {

    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")

    val admin = Admin.create(props)

    admin.deleteTopics(Seq(
      Topics.TASKS,
      Topics.SUBSCRIPTIONS,
      Broker.TOPIC
    ).asJava).all().get()

    admin.close()

  }

}
