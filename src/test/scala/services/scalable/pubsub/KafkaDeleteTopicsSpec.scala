package services.scalable.pubsub

import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

class KafkaDeleteTopicsSpec extends AnyFlatSpec {

  val logger = LoggerFactory.getLogger(this.getClass)

  "it " should " delete topics successfully" in {

    logger.info(s"kafka recreation result: ${Await.result(KafkaAdminHelper.delete(Config.topics.keys.toSeq), Duration.Inf)}")

  }

}
