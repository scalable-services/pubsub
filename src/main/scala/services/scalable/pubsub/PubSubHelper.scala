package services.scalable.pubsub

import com.google.cloud.pubsub.v1.{SubscriptionAdminClient, SubscriptionAdminSettings, TopicAdminClient, TopicAdminSettings}
import com.google.pubsub.v1.{ListTopicsRequest, ProjectSubscriptionName, PushConfig, TopicName}
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future, Promise}

object PubSubHelper {

  val logger = LoggerFactory.getLogger(this.getClass)

  val subAdminSettings = SubscriptionAdminSettings.newBuilder()
    .setCredentialsProvider(GOOGLE_CREDENTIALS_PROVIDER).build()

  val topicAdminSettings = TopicAdminSettings.newBuilder()
    .setCredentialsProvider(GOOGLE_CREDENTIALS_PROVIDER).build()

  val subAdmin = SubscriptionAdminClient.create(subAdminSettings)
  val topicAdmin = TopicAdminClient.create(topicAdminSettings)

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

  def createTopics(topics: Map[String, TopicName])(implicit ec: ExecutionContext): Future[Boolean] = {
    var tasks = Seq.empty[Future[Boolean]]

    topics.foreach { case (topic, name) =>
      val pr = Promise[Boolean]()

      ec.execute{ () =>
        val topic = topicAdmin.createTopic(name)
        logger.info(s"created topic: ${topic}")

        pr.success(true)
      }

      tasks = tasks :+ pr.future
    }

    Future.sequence(tasks).map(!_.contains(false))
  }

  def deleteTopics(topics: Seq[String])(implicit ec: ExecutionContext): Future[Boolean] = {
    var tasks = Seq.empty[Future[Boolean]]

    topics.foreach { t =>
      val pr = Promise[Boolean]()

      ec.execute { () =>
        topicAdmin.deleteTopic(t)
        logger.info(s"deleted topic: ${t}")

        pr.success(true)
      }

      tasks = tasks :+ pr.future
    }

    Future.sequence(tasks).map(!_.contains(false))
  }

  def createSubscriptions(subscriptions: Map[String, TopicName])(implicit ec: ExecutionContext): Future[Boolean] = {
    var tasks = Seq.empty[Future[Boolean]]

    subscriptions.foreach { case (s, topicName) =>
      val pr = Promise[Boolean]()

      ec.execute { () =>
        val subscription = subAdmin.createSubscription(ProjectSubscriptionName.of(Config.projectId, s), topicName,
          PushConfig.getDefaultInstance(), 10)
        logger.info(s"created push subscription: ${subscription}\n")

        pr.success(true)
      }

      tasks = tasks :+ pr.future
    }

    Future.sequence(tasks).map(!_.contains(false))
  }

  def deleteSubscriptions(subscriptions: Seq[String])(implicit ec: ExecutionContext): Future[Boolean] = {
    var tasks = Seq.empty[Future[Boolean]]

    subscriptions.foreach { s =>
      val pr = Promise[Boolean]()

      ec.execute { () =>
        subAdmin.deleteSubscription(s)
        logger.info(s"deleted push subscription: ${s}\n")

        pr.success(true)
      }

      tasks = tasks :+ pr.future
    }

    Future.sequence(tasks).map(!_.contains(false))
  }

}
