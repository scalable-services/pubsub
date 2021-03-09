package services.scalable.pubsub

import akka.actor.typed.{ActorRef, ActorSystem}
import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.scaladsl.AskPattern.Askable
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, EntityTypeKey}
import akka.util.Timeout

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import services.scalable.pubsub.grpc._

import scala.util.{Failure, Success}

class GreeterServiceImpl(sys: ActorSystem[_], sharding: ClusterSharding)
                        (implicit ec: ExecutionContext) extends GreeterService {

  implicit val timeout = Timeout(3 seconds)
  implicit val scheduler = sys.scheduler

  override def sayHello(req: HelloRequest): Future[HelloResponse] = {

    println(s"${Console.GREEN_B}RECEIVED REQUEST: ${req}${Console.RESET}")

    //Future.successful(HelloResponse(s"Hello, ${req.name}"))

    /*val ref = sharding.entityRefFor(Main.TypeKey, req.name)

    ref.ask { actor =>
      Greeter.Greet(req.name, actor)
    }*/

    /*val serviceKey = ServiceKey[Worker.Command]("w0")

    sys.receptionist.ask[Receptionist.Listing] { actor =>
      Receptionist.Find(serviceKey, actor)
    }.onComplete {
      case Success(listing) =>

        var instances = Seq.empty[ActorRef[Worker.Command]]

        listing.getServiceInstances(serviceKey).forEach { ref =>
          instances = instances :+ ref
        }

        instances.head ! Worker.Ping

      case Failure(ex) => ex.printStackTrace()
    }*/

    Future.successful(HelloResponse(s"Hello, ${req.name}"))
  }
}
