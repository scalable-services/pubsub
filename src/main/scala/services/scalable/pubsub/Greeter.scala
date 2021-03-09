package services.scalable.pubsub

import akka.actor.typed.ActorRef
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.sharding.typed.scaladsl.EntityContext

import services.scalable.pubsub.grpc._

object Greeter {

  sealed trait Command

  final case class Greet(name: String, replyTo: ActorRef[HelloResponse]) extends Command

  def apply(entityId: String, ectx: EntityContext[Command]) = Behaviors.setup[Greeter.Command] { ctx =>

    println(s"${Console.GREEN_B}shard id active ${ectx.shard.path}${Console.RESET}")

    Behaviors.receiveMessage[Command] {
      case Greet(name, replyTo) =>

        replyTo ! HelloResponse(s"Hello, ${name} from entity id ${entityId} at shard ${ectx.shard}!")

        Behaviors.same
    }
  }

}
