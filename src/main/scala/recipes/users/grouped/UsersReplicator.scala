package recipes.users.grouped

import java.util.UUID

import akka.cluster.Cluster
import akka.cluster.ddata.Replicator
import recipes.users.crdt.VersionedUsers
import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.cluster.ddata.Replicator.{ReadLocal, UpdateFailure, WriteLocal}
import recipes.users._

object UsersReplicator {
  case class UserAddCtx(userId: String)
  case class UserUpdateCtx(correlationId: String)

  def props(tenant: String, system: ActorSystem) =
    Props(new UsersReplicator(tenant, system))
}

class UsersReplicator(tenant: String, system: ActorSystem) extends Actor with ActorLogging {
  import UsersReplicator._

  val wc = WriteLocal
  val rc = ReadLocal

  implicit val cluster = Cluster(context.system)

  val replicator = akka.cluster.ddata.DistributedData(context.system).replicator

  private val Key = UserSegment(tenant)

  private val address = cluster.selfUniqueAddress.address

  def addUser: Receive = {
    var addUsersInFly = Map.empty[String, Long]

    {
      case CreateUser(userId, login, isActive) =>
        val ctx = UserAddCtx(userId)
        val user = User(userId, login, Node(address.host.get, address.port.get), isActive)
        addUsersInFly = addUsersInFly + (userId -> System.nanoTime)
        replicator ! Replicator.Update(Key, VersionedUsers(Node(address.host.get, address.port.get)), wc, Some(ctx)) {
          _.add(user, Node(address.host.get, address.port.get))
        }

      /*replicator ! Replicator.Update(key, ORMap.empty[String, CrdtUsers], wc, Some(UserCreatedCtx(shardId, user))) {
        map: ORMap[String, CrdtUsers] =>
          map.get(shardId.toString).fold(map.put(cluster, shardId.toString, CrdtUsers().add(user, version))) { users =>
            map.put(cluster, shardId.toString, users.add(user, version))
          }
      }*/

      case Replicator.UpdateSuccess(key, Some(UserAddCtx(userId))) =>
        val id = addUsersInFly.get(userId)
        require(id.isDefined, s"Couldn't find addUsersInFly ${userId}")
        addUsersInFly = addUsersInFly - userId
        log.info(s"Addition by key $key user: ${userId} has been added successfully")
      case Replicator.UpdateTimeout(key, Some(UserAddCtx(userId))) =>
        log.info(s"Addition by $key user: ${userId} has failed by timeout")
      case r: UpdateFailure[_] =>
        log.info(s"Addition by ${r.key} UpdateFailure: ${r.getClass.getName}")
    }
  }

  def updateUser: Receive = {
    var updateUsersInFly = Map.empty[String, String]

    {
      case c: ActivateUser =>
        val ctx = UserUpdateCtx(UUID.randomUUID.toString)
        replicator ! Replicator.Update(Key, VersionedUsers(Node(address.host.get, address.port.get)), wc, Some(ctx)) { users =>
          log.info(s"Update:ActivateUser - ${c.userId} by [${Key}: ${c.userId}] is scheduled")
          updateUsersInFly =  updateUsersInFly + (ctx.correlationId -> c.userId)

          val maybeRegisterForUpdate = users.elements.find(_.id == c.userId)
          require(maybeRegisterForUpdate.isDefined, "Couldn't find users for update")
          val currentUser = maybeRegisterForUpdate.get
          val updatedUser = currentUser.copy(active = c.isActive)
          users.update(currentUser, updatedUser, Node(address.host.get, address.port.get))
        }

      case c: LoginUser =>
        val ctx = UserUpdateCtx("LoginUser-" + UUID.randomUUID.toString)
        replicator ! Replicator.Update(Key, VersionedUsers(Node(address.host.get, address.port.get)), wc, Some(ctx)) { users =>
          log.info(s"Update:LoginUser - ${c.userId} by [${Key}: ${c.userId}] is scheduled")
          updateUsersInFly = updateUsersInFly + (ctx.correlationId -> c.userId)
          val maybeRegisterForUpdate = users.elements.find(_.id == c.userId)
          require(maybeRegisterForUpdate.isDefined, "Couldn't find users for update")
          val currentUser = maybeRegisterForUpdate.get
          val updatedUser = currentUser.copy(online = c.isLogin)
          users.update(currentUser, updatedUser, Node(address.host.get, address.port.get))
        }

      /*case UpdateUser(userId, login, updated) =>
      replicator ! Replicator.Update(key, ORMap.empty[String, CrdtUsers], wc,
        Some(UserUpdatedCtx(shardId, userId, login, updated))) { map =>
          val maybeRegisterForUpdate = map.get(shardId.toString)
          require(maybeRegisterForUpdate.isDefined, "Couldn't find users")
          val updatedUser = User(userId, login, updated)
          val maybeDeleted = maybeRegisterForUpdate.get.elements.find(_.id == userId)
          require(maybeRegisterForUpdate.isDefined, "Couldn't find user for deletion")
          val deletedUser = maybeDeleted.get
          map.put(cluster, shardId.toString, maybeRegisterForUpdate.get.update(deletedUser, updatedUser, version))
        }*/

      case Replicator.UpdateSuccess(Key, Some(UserUpdateCtx(correlationId))) =>
        val userId = updateUsersInFly.get(correlationId)
        require(userId.isDefined, s"Couldn't find updateUsersInFly by ${correlationId}")
        updateUsersInFly = updateUsersInFly - correlationId
        log.info(s"Update [${userId.get} - ${correlationId}] has been completed")

      case Replicator.UpdateTimeout(Key, Some(UserUpdateCtx(correlationId))) =>
        log.info(s"UpdateTimeout for ${correlationId}")
        updateUsersInFly = updateUsersInFly - correlationId

      case r: UpdateFailure[_] =>
        log.info(s"Update has failed: ${r.getClass.getName}")
    }
  }

  override def receive: Receive = addUser orElse updateUser
}