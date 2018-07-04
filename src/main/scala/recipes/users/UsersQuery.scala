package recipes.users

import akka.actor.{Actor, ActorLogging, ActorRef, Kill, Props}

object UsersQuery {
  def props(tenants: List[String], reader: ActorRef) = Props(new UsersQuery(tenants, reader))
}

class UsersQuery(shardIds: List[String], reader: ActorRef) extends Actor with ActorLogging {
  if (shardIds.isEmpty) self ! Kill else reader ! shardIds.head

  def request(rest: List[String]) = {
    rest match {
      case Nil =>
        self ! Kill
      case shardId :: tail =>
        reader ! shardId
        context become await(shardId, tail)
    }
  }

  def await(tenant: String, rest: List[String]): Receive = {
    case usersByShard: Set[User]@unchecked =>
      //expectation
      log.info("All have been updated: " +
        (usersByShard.nonEmpty &&
          usersByShard.forall(_.active == true) &&
          usersByShard.forall(_.online == true)))

      request(rest)
  }

  override def receive: Receive = await(shardIds.head, shardIds.tail)
}