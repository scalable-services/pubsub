package services.scalable.pubsub

import org.scalatest.flatspec.AnyFlatSpec

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class PubSubSpec extends AnyFlatSpec {

  "it" should "create and delete topics" in {

    Await.result(PubSubHelper.createTopics(Config.topics), Duration.Inf)
    Await.result(PubSubHelper.createSubscriptions(Config.subscriptions), Duration.Inf)

  }

}
