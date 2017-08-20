package recipes.users

import akka.actor.{ Actor, ActorLogging, ActorRef, Props }
import akka.cluster.ddata.{ DistributedData, Replicator }
import akka.cluster.ddata.Replicator.{ Get, GetFailure, GetSuccess, ReadConsistency }

object UsersReader {
  case class ReaderContext(shardId: Long, sender: ActorRef)
  def props(rc: ReadConsistency) = Props(new UsersReader(rc))
}

class UsersReader(readConsistency: ReadConsistency) extends Actor with ActorLogging with MultiMapsPartitioner {
  import UsersReader._
  val replicator = DistributedData(context.system).replicator

  override def receive: Receive = {
    case shardId: Long =>
      val key = getKey(shardId)
      log.info(s"Read users for [${key} shardId:$shardId]")
      replicator ! Get(key, readConsistency, Some(ReaderContext(shardId, sender)))
    case r @ GetSuccess(k: ReplicatedKey, Some(ReaderContext(shardId, replyTo))) =>
      r.get(k)
        .get(shardId.toString)
        .fold(replyTo ! Set.empty[User]) { users => replyTo ! users.elements }
    case Replicator.NotFound(k: ReplicatedKey, Some(ReaderContext(shardId, replyTo))) =>
      log.info(s"NotFound by [${k} shardId:$shardId]")
      replyTo ! Set.empty[User]
    case GetFailure(k:ReplicatedKey, Some(ReaderContext(shardId, replyTo))) =>
      log.info(s"Read failure, try to read it from local replica {}", shardId)
  }
}
