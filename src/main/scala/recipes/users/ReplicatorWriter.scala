package recipes.users

import akka.cluster.Cluster
import recipes.users.crdt.{CrdtUsers, VersionedUsers}
import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import recipes.users.UserWriter.{CreateUser, UpdateUser}
import akka.cluster.ddata.{ORMap, Replicator, ReplicatorSettings}
import akka.cluster.ddata.Replicator.{ReadLocal, UpdateFailure, WriteLocal}
import com.typesafe.config.ConfigFactory

object ReplicatorWriter {
  sealed trait ReqCtx
  case class UserCreatedCtx(shardId: Long, user: User)
  case class UserUpdatedCtx(shardId: Long, userId: Long, login: String, active: Boolean)

  def props(shardId: Long, s: ActorSystem) = Props(new ReplicatorWriter(shardId, s))
}

class ReplicatorWriter(shardId: Long, s: ActorSystem) extends Actor with ActorLogging with MultiMapsPartitioner {
  import ReplicatorWriter._
  var version = 1

  val wc = WriteLocal
  val rc = ReadLocal

  implicit val cluster = Cluster(context.system)

  //https://groups.google.com/forum/#!topic/akka-user/MO-4XhwhAN0
  val name = s"replicator-$shardId"

  val config = ConfigFactory.parseString(
    s"""
      | name = $name
      | role = replicaA
      | gossip-interval = 1 s
      | use-dispatcher = ""
      | notify-subscribers-interval = 500 ms
      | max-delta-elements = 1000
      | pruning-interval = 120 s
      | max-pruning-dissemination = 300 s
      | pruning-marker-time-to-live = 6 h
      | serializer-cache-time-to-live = 10s
      | delta-crdt {
      |   enabled = on
      |   max-delta-size = 1000
      | }
      |
      | durable {
      |  keys = []
      |  pruning-marker-time-to-live = 10 d
      |  store-actor-class = akka.cluster.ddata.LmdbDurableStore
      |  use-dispatcher = akka.cluster.distributed-data.durable.pinned-store
      |  pinned-store {
      |    executor = thread-pool-executor
      |    type = PinnedDispatcher
      |  }
      |
      |  lmdb {
      |    dir = "ddata"
      |    map-size = 100 MiB
      |    write-behind-interval = off
      |  }
      | }
    """.stripMargin)

  val settings = ReplicatorSettings(config)
  val replicator = s.actorOf(Replicator.props(settings), name)
  //val replicator = akka.cluster.ddata.DistributedData(context.system).replicator

  val key = getKey(shardId)

  def addUser: Receive = {
    case CreateUser(userId: Long, login: String, isActive: Boolean) =>
      val user = User(userId, login, isActive)
      
      /*replicator ! Replicator.Update(key, ORMap.empty[String, VersionedUsers], wc, Some(UserCreatedCtx(shardId, user))) { map =>
        map
          .get(shardId.toString)
          .fold(map + (shardId.toString -> VersionedUsers(cluster.selfUniqueAddress).add(user, cluster.selfUniqueAddress))) { users =>
            map + (shardId.toString -> users.add(user, cluster.selfUniqueAddress))
          }
      }*/

      replicator ! Replicator.Update(key, ORMap.empty[String, CrdtUsers], wc, Some(UserCreatedCtx(shardId, user))) {
        map: ORMap[String, CrdtUsers] =>
          map.get(shardId.toString).fold(map.put(cluster, shardId.toString, CrdtUsers().add(user, version))) { users =>
            map.put(cluster, shardId.toString, users.add(user, version))
          }
      }

    case Replicator.UpdateSuccess(key, Some(UserCreatedCtx(shardId, user))) =>
      version += 1
      log.info(s"Addition: user:${user} by [${key} shardId:$shardId] has been added successfully")
    case Replicator.UpdateTimeout(key, Some(UserCreatedCtx(shardId, user))) =>
      log.info(s"Addition: user${user}  by [${key} shardId:$shardId] has failed by timeout", user, key)
    case r: UpdateFailure[_] =>
      log.info(s"Addition: UpdateFailure by ${r.key} ${r.getClass.getName}")
  }

  def updateUser: Receive = {
    case UpdateUser(userId, login, updated) =>
      replicator ! Replicator.Update(key, ORMap.empty[String, CrdtUsers], wc,
        Some(UserUpdatedCtx(shardId, userId, login, updated))) { map =>
          val maybeRegisterForUpdate = map.get(shardId.toString)
          require(maybeRegisterForUpdate.isDefined, "Couldn't find users")
          val updatedUser = User(userId, login, updated)
          val maybeDeleted = maybeRegisterForUpdate.get.elements.find(_.id == userId)
          require(maybeRegisterForUpdate.isDefined, "Couldn't find  user for deletion")
          val deletedUser = maybeDeleted.get
          map.put(cluster, shardId.toString, maybeRegisterForUpdate.get.update(deletedUser, updatedUser, version))
        }

    case Replicator.UpdateSuccess(key, Some(UserUpdatedCtx(_, _, _, _))) =>
      log.info(s"Update by [${key} shardId:$shardId] has been added successfully")
      version += 1
    case Replicator.UpdateTimeout(key, Some(UserCreatedCtx(shardId, _))) =>
      log.info(s"Update by [$key shard:$shardId] timeout")
    case r: UpdateFailure[_] =>
      log.info(s"Update by shard:$shardId has failed: ${r.getClass.getName}")
  }

  override def receive: Receive = addUser orElse updateUser
}

