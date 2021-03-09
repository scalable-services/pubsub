package services.scalable.pubsub

//#import


import akka.actor.typed.ActorSystem
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

import services.scalable.pubsub.grpc._

class GreeterServer(system: ActorSystem[_], sharding: ClusterSharding) {

  def run(port: Int): Future[Http.ServerBinding] = {
    implicit val sys = system
    implicit val ec: ExecutionContext = system.executionContext

    val service: HttpRequest => Future[HttpResponse] =
      GreeterServiceHandler(new GreeterServiceImpl(system, sharding))

    val bound: Future[Http.ServerBinding] = Http(system)
      .newServerAt(interface = "127.0.0.1", port = port)
      .bind(service)
      .map(_.addToCoordinatedShutdown(hardTerminationDeadline = 10.seconds))

    bound.onComplete {
      case Success(binding) =>
        val address = binding.localAddress
        println("gRPC server bound to {}:{}", address.getHostString, address.getPort)
      case Failure(ex) =>
        println("Failed to bind gRPC endpoint, terminating system", ex)
        system.terminate()
    }

    bound
  }
  //#server

}
//#server
