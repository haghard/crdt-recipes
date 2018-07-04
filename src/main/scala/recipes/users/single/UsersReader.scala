package recipes.users.single

import akka.actor.{Actor, ActorLogging, ActorRef, Props, Stash}
import akka.cluster.ddata.Replicator.{Get, GetFailure, GetSuccess, ReadConsistency}
import akka.cluster.ddata.{DistributedData, Replicator}
import recipes.users.{User, UsersKey}

object UsersReader {

  case class ReaderContext(sender: ActorRef)

  def props(rc: ReadConsistency) = Props(new UsersReader(rc))
}

class UsersReader(readConsistency: ReadConsistency) extends Actor with ActorLogging with Stash {
  import UsersReader._

  val key = UsersKey

  val replicator = DistributedData(context.system).replicator

  override def receive: Receive =
    readQuery

  def readQuery: Receive = {
    case tenant: String =>
      log.info(s"Read users by [$tenant]")
      replicator ! Get(key, readConsistency, Some(ReaderContext(sender)))
      context.become(awaitResponse(key, tenant))
  }

  def awaitResponse(Key: UsersKey.type, tenant: String): Receive = {
    case r @ GetSuccess(Key, Some(ReaderContext(replyTo))) =>
      val tenantUsers = r.get(Key).elements.filter(_.id.startsWith(tenant))
      log.info(s"Tenant: $tenant - ${tenantUsers.mkString(",")}")
      replyTo ! tenantUsers
      unstashAll()
      context.become(readQuery)
    case Replicator.NotFound(Key, Some(ReaderContext(replyTo))) =>
      log.info(s"NotFound by [${Key} tenant:$tenant]")
      replyTo ! Set.empty[User]
      unstashAll()
      context.become(readQuery)
    case GetFailure(Key, Some(ReaderContext(replyTo))) =>
      log.info(s"Read failure, try to read it from local replica {}", tenant)
      replyTo ! Set.empty[User]
      unstashAll()
      context.become(readQuery)
    case _: String =>
      stash()
  }
}
