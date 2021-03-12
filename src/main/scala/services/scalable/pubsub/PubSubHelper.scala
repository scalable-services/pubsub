package services.scalable.pubsub

import com.google.cloud.pubsub.v1.{SubscriptionAdminClient, TopicAdminClient, TopicAdminSettings}
import com.google.pubsub.v1.{ListTopicsRequest, ProjectSubscriptionName, PushConfig, TopicName}
import org.slf4j.LoggerFactory

object PubSubHelper {

  val logger = LoggerFactory.getLogger(this.getClass)

  val subAdmin = SubscriptionAdminClient.create()
  val topicAdmin = TopicAdminClient.create()

  /*def listTopics(): Seq[String] = {
    var topics = Seq.empty[String]

    topicAdmin.listTopics(Config.projectName).iterateAll().forEach{ topic =>
      val name = topic.getName
      if(name.startsWith(Config.PROJECT_PREFIX)){
        topics :+= name
      }
    }

    topics
  }

  def listSubscriptions(): Seq[String] = {
    var subscriptions = Seq.empty[String]

    subAdmin.listSubscriptions(Config.projectId).iterateAll().forEach{ s =>
      val name = s.getName
      if(name.startsWith(Config.PROJECT_PREFIX)){
        subscriptions :+= name
      }
    }

    subscriptions
  }*/

  def createTopics(topics: Map[String, TopicName]): Unit = {
    topics.foreach { case (topic, name) =>
      val topic = topicAdmin.createTopic(name)
      logger.info(s"created topic: ${topic}")
    }
  }

  def deleteTopics(topics: Seq[String]): Unit = {
    topics.foreach { t =>
      topicAdmin.deleteTopic(t)
      logger.info(s"deleted topic: ${t}")
    }
  }

  def createSubscriptions(subscriptions: Map[String, TopicName]): Unit = {
    subscriptions.foreach { case (s, topicName) =>
      val subscription = subAdmin.createSubscription(ProjectSubscriptionName.of(Config.projectId, s), topicName,
        PushConfig.getDefaultInstance(), 10)
      logger.info(s"created push subscription: ${subscription}\n")
    }
  }

  def deleteSubscriptions(subscriptions: Seq[String]): Unit = {
    subscriptions.foreach { s =>
      subAdmin.deleteSubscription(s)
      logger.info(s"deleted push subscription: ${s}\n")
    }
  }

}
