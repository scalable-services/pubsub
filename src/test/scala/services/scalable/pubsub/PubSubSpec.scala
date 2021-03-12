package services.scalable.pubsub

import org.scalatest.flatspec.AnyFlatSpec

class PubSubSpec extends AnyFlatSpec {

  "it" should "create and delete topics" in {

    PubSubHelper.createTopics(Config.topics)
    PubSubHelper.createSubscriptions(Config.subscriptions)

  }

}
