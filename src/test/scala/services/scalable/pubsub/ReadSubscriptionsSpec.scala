package services.scalable.pubsub

import com.google.common.primitives.UnsignedBytes
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory
import services.scalable.index._
import services.scalable.index.impl.{CassandraStorage, DefaultCache}

import java.util.concurrent.ThreadLocalRandom
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.{Failure, Success}

class ReadSubscriptionsSpec extends AnyFlatSpec {

  val logger = LoggerFactory.getLogger(this.getClass)

  val rand = ThreadLocalRandom.current()

  "random leaf greater than data " must "be equal to test data" in {

    type K = Array[Byte]
    type V = Array[Byte]
    type S = String

    implicit val ord = new Ordering[K] {
      val comp = UnsignedBytes.lexicographicalComparator()
      override def compare(x: K, y: K): Int = comp.compare(x, y)
    }

    val indexId = "test_subscribers"

    implicit val cache = new DefaultCache()
    implicit val storage = new CassandraStorage("indexes", truncate = false)

    val ctx = Await.result(storage.load(indexId), Duration.Inf)

    val index = new Index[S, K, V]()(global, ctx)

    val subscribers = Await.result(index.inOrder(), Duration.Inf).map{case (k, v) => new String(k) -> new String(v)}

    logger.info(s"${ctx.root}")
    logger.info(s"\n${Console.MAGENTA_B}length: ${subscribers.length} => ${subscribers}${Console.RESET}\n")
  }

}
