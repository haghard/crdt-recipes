package recipes.users

import akka.cluster.Cluster
import recipes.users.crdt.VersionedUsers
import akka.actor.{Actor, ActorLogging, Props}
import recipes.users.Journal.{CreateUser, UpdateUser}
import akka.cluster.ddata.{DistributedData, ORMap, Replicator}
import akka.cluster.ddata.Replicator.{ReadLocal, UpdateFailure, WriteLocal}

object UsersWriter {
  sealed trait ReqCtx
  case class UserCreatedCtx(shardId: Long, user: User)
  case class UserUpdatedCtx(shardId: Long, userId: Long, login: String, active: Boolean)

  def props(shardId: Long) = Props(new UsersWriter(shardId))
}

class UsersWriter(shardId: Long) extends Actor with ActorLogging {
  import UsersWriter._
  var version = 1

  val wc = WriteLocal
  val rc = ReadLocal

  implicit val cluster = Cluster(context.system)
  val replicator = DistributedData(context.system).replicator

  def getKey(shardId: Long) = s"shard.${shardId}"

  def addUser: Receive = {
    case CreateUser(userId: Long, login: String, isActive: Boolean) =>
      val user = User(userId, login, isActive)
      val key = getKey(shardId)
      replicator ! Replicator.Update(UsersKey, ORMap.empty[String, VersionedUsers], wc, Some(UserCreatedCtx(shardId, user))) { map =>
        map.get(key).fold(map.put(cluster, key, VersionedUsers().add(user, version))) { users =>
          map.put(cluster, key, users.add(user, version))
        }
      }

    case Replicator.UpdateSuccess(key, Some(UserCreatedCtx(_, user))) =>
      version += 1
      log.info(s"Addition: user ${user} by key ${key} has been added successfully")
    case Replicator.UpdateTimeout(key, Some(UserCreatedCtx(_, user))) =>
      log.info(s"Addition: user {} by key {} has failed by timeout", user, key)
    case r: UpdateFailure[_] =>
      log.info(s"Addition: UpdateFailure by ${r.key} ${r.getClass.getName}")
  }

  def updateUser: Receive = {
    case UpdateUser(userId, login, updated) =>
      val key = getKey(shardId)
      replicator ! Replicator.Update(UsersKey, ORMap.empty[String, VersionedUsers], wc,
        Some(UserUpdatedCtx(shardId, userId, login, updated))) { map =>
          val maybeRegisterForUpdate = map.get(key)
          require(maybeRegisterForUpdate.isDefined, "Couldn't find users")
          val updatedUser = User(userId, login, updated)
          val maybeDeleted = maybeRegisterForUpdate.get.elements.find(_.id == userId)
          require(maybeRegisterForUpdate.isDefined, "Couldn't find  user fr deletion")
          val deletedUser = maybeDeleted.get
          map.put(cluster, key, maybeRegisterForUpdate.get.update(deletedUser, updatedUser, version))
        }

    case Replicator.UpdateSuccess(UsersKey, Some(UserUpdatedCtx(_, _, _, _))) =>
      version += 1
    case Replicator.UpdateTimeout(key, Some(UserCreatedCtx(_, user))) =>
      log.info(s"Update: user {} by key {} has been added successfully", user, key)
    case r: UpdateFailure[_] =>
      log.info(s"UserAdded UpdateFailure by ${r.key} ${r.getClass.getName}")
  }

  override def receive: Receive = addUser orElse updateUser
}

