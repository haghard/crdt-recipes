package recipes.users.grouped

import akka.actor.{Actor, ActorLogging, ActorRef, Props, Stash}
import akka.cluster.ddata.Replicator.{Get, GetFailure, GetSuccess, ReadConsistency}
import akka.cluster.ddata.{DistributedData, Replicator}
import recipes.users.{User, UserSegment}

object UsersReader {

  case class ReaderContext(shardId: Long, sender: ActorRef)

  case class ReadTenantContext(tenant: String, sender: ActorRef)

  def props(rc: ReadConsistency) = Props(new UsersReader(rc))
}

class UsersReader(readConsistency: ReadConsistency) extends Actor with ActorLogging
  //with MultiMapsPartitioner {
  with Stash {

  import UsersReader._
  val replicator = DistributedData(context.system).replicator

  override def receive: Receive =
    readQuery

  def readQuery: Receive = {
    case tenant: String =>
      val key = UserSegment(tenant)
      log.info(s"Read users for [$tenant]")
      replicator ! Get(key, readConsistency, Some(ReadTenantContext(tenant, sender)))
      context.become(awaitResponse(key))
  }

  def awaitResponse(Key: UserSegment): Receive = {
    case r @ GetSuccess(Key, Some(ReadTenantContext(tenant, replyTo))) =>
      val users = r.get(Key).elements
      log.info(s"Tenant: $tenant - ${users.mkString(",")}")
      replyTo ! users
      unstashAll()
      context.become(readQuery)
    case Replicator.NotFound(Key, Some(ReadTenantContext(tenant, replyTo))) =>
      log.info(s"NotFound by [${Key} tenant:$tenant]")
      replyTo ! Set.empty[User]
      unstashAll()
      context.become(readQuery)
    case GetFailure(Key, Some(ReadTenantContext(tenant, replyTo))) =>
      log.info(s"Read failure, try to read it from local replica {}", tenant)
      replyTo ! Set.empty[User]
      unstashAll()
      context.become(readQuery)
    case _: String =>
      stash()
  }

  /*def receive0: Receive = {
    case id: Long =>
      val key = getBucket(id)
      log.info(s"Read users for [$id] mapped into ${key}]")
      replicator ! Get(key, readConsistency, Some(ReaderContext(id, sender)))
    case r @ GetSuccess(k: ReplicatedKey, Some(ReaderContext(shardId, replyTo))) =>
      val users = r.get(k).elements
      log.info(s"Key:$k - ${users.mkString(",")}")
      replyTo ! users

      /*r.get(k)
        .get(shardId.toString)
        .fold(replyTo ! Set.empty[User]) { users => replyTo ! users.elements }
        */
    case Replicator.NotFound(k: ReplicatedKey, Some(ReaderContext(shardId, replyTo))) =>
      log.info(s"NotFound by [${k} shardId:$shardId]")
      replyTo ! Set.empty[User]
    case GetFailure(k: ReplicatedKey, Some(ReaderContext(shardId, replyTo))) =>
      log.info(s"Read failure, try to read it from local replica {}", shardId)
  }*/
}
