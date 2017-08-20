package recipes.users

import akka.actor.{ Actor, ActorLogging, ActorRef, Props }
import akka.cluster.ddata.{ DistributedData, Replicator }
import akka.cluster.ddata.Replicator.{ Get, GetFailure, GetSuccess, ReadConsistency }

object UsersReader {
  case class ReaderContext(shardId: Long, sender: ActorRef)
  def props(rc: ReadConsistency) = Props(new UsersReader(rc))
}

class UsersReader(readConsistency: ReadConsistency) extends Actor with ActorLogging {
  import UsersReader._
  val replicator = DistributedData(context.system).replicator

  def getKey(shardId: Long) = s"shard.${shardId}"

  override def receive: Receive = {
    case shardId: Long =>
      log.info(s"Read users for ${shardId}")
      replicator ! Get(UsersKey, readConsistency, Some(ReaderContext(shardId, sender)))
    case r @ GetSuccess(UsersKey, Some(ReaderContext(shardId, replyTo))) =>
      r.get(UsersKey)
        .get(getKey(shardId))
        .fold(replyTo ! Set.empty[User]) { users => replyTo ! users.elements }
    case Replicator.NotFound(UsersKey, Some(ReaderContext(_, replyTo))) =>
      log.info("NotFound")
      replyTo ! Set.empty[User]
    case GetFailure(UsersKey, Some(ReaderContext(shardId, replyTo))) =>
      log.info(s"Read failure, try to read it from local replica {}", shardId)
  }
}
