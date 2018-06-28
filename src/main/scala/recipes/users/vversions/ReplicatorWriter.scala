package recipes.users.vversions

import java.util.UUID

import akka.cluster.Cluster
import akka.cluster.ddata.Replicator
import recipes.users.crdt.VersionedUsers
import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.cluster.ddata.Replicator.{ReadLocal, UpdateFailure, WriteLocal}
import recipes.users.vversions.UserWriter.{ActivateUser, CreateUser, LoginUser}
import recipes.users.{Node, User, VersionVectorPartitioner}

object ReplicatorWriter {
  case class UserCtx(userId: String)
  case class UserUpdateCtx(correlationId: String)

  def props(system: ActorSystem) = Props(new ReplicatorWriter(system))
}

class ReplicatorWriter(system: ActorSystem) extends Actor with ActorLogging
  //with MultiMapsPartitioner {
  with VersionVectorPartitioner {
  import ReplicatorWriter._

  val wc = WriteLocal
  val rc = ReadLocal

  implicit val cluster = Cluster(context.system)

  val replicator = akka.cluster.ddata.DistributedData(context.system).replicator

  private val Key = getBucket(0)
  private val A = cluster.selfUniqueAddress.address

  def addUser: Receive = {
    var addUsersInFly = Map.empty[String, Long]

    {
      case CreateUser(userId, login, isActive) =>
        val ctx = UserCtx(userId)
        val user = User(userId, login, Node(A.host.get, A.port.get), isActive)
        addUsersInFly = addUsersInFly + (userId -> System.nanoTime)
        replicator ! Replicator.Update(Key, VersionedUsers(Node(A.host.get, A.port.get)), wc, Some(ctx)) {
          _.add(user, Node(A.host.get, A.port.get))
        }

      /*replicator ! Replicator.Update(key, ORMap.empty[String, CrdtUsers], wc, Some(UserCreatedCtx(shardId, user))) {
        map: ORMap[String, CrdtUsers] =>
          map.get(shardId.toString).fold(map.put(cluster, shardId.toString, CrdtUsers().add(user, version))) { users =>
            map.put(cluster, shardId.toString, users.add(user, version))
          }
      }*/

      case Replicator.UpdateSuccess(Key, Some(UserCtx(userId))) =>
        val id = addUsersInFly.get(userId)
        require(id.isDefined, s"Couldn't find addUsersInFly ${userId}")
        addUsersInFly = addUsersInFly - userId
        log.info(s"Addition:${userId} has been added successfully")
      case Replicator.UpdateTimeout(Key, Some(UserCtx(userId))) =>
        log.info(s"Addition:${userId}  has failed by timeout")
      case r: UpdateFailure[_] =>
        log.info(s"Addition: UpdateFailure by ${r.key} ${r.getClass.getName}")
    }
  }


  def updateUser: Receive = {
    var updateUsersInFly = Map.empty[String, String]

    {
      case c: ActivateUser =>
        val ctx = UserUpdateCtx(UUID.randomUUID.toString)
        replicator ! Replicator.Update(Key, VersionedUsers(Node(A.host.get, A.port.get)), wc, Some(ctx)) { users =>
          log.info(s"Update:ActivateUser - ${c.userId} by [${Key}: ${c.userId}] is scheduled")
          updateUsersInFly =  updateUsersInFly + (ctx.correlationId -> c.userId)

          val maybeRegisterForUpdate = users.elements.find(_.id == c.userId)
          require(maybeRegisterForUpdate.isDefined, "Couldn't find users for update")
          val currentUser = maybeRegisterForUpdate.get
          val updatedUser = currentUser.copy(active = c.isActive)
          users.update(currentUser, updatedUser, Node(A.host.get, A.port.get))
        }

      case c: LoginUser =>
        val ctx = UserUpdateCtx("LoginUser-" + UUID.randomUUID.toString)
        replicator ! Replicator.Update(Key, VersionedUsers(Node(A.host.get, A.port.get)), wc, Some(ctx)) { users =>
          log.info(s"Update:LoginUser - ${c.userId} by [${Key}: ${c.userId}] is scheduled")
          updateUsersInFly = updateUsersInFly + (ctx.correlationId -> c.userId)
          val maybeRegisterForUpdate = users.elements.find(_.id == c.userId)
          require(maybeRegisterForUpdate.isDefined, "Couldn't find users for update")
          val currentUser = maybeRegisterForUpdate.get
          val updatedUser = currentUser.copy(online = c.isLogin)
          users.update(currentUser, updatedUser, Node(A.host.get, A.port.get))
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

      case Replicator.UpdateSuccess(Key, Some(UserUpdateCtx(uuid))) =>
        val userId = updateUsersInFly.get(uuid)
        require(userId.isDefined, s"Couldn't find updateUsersInFly by ${uuid}")
        updateUsersInFly = updateUsersInFly - uuid
        log.info(s"Update [${userId.get} - ${uuid}] has been completed")

      case Replicator.UpdateTimeout(Key, Some(UserUpdateCtx(uuid))) =>
        log.info(s"UpdateTimeout for ${uuid}")
        updateUsersInFly = updateUsersInFly - uuid

      case r: UpdateFailure[_] =>
        log.info(s"Update has failed: ${r.getClass.getName}")
    }
  }

  override def receive: Receive = addUser orElse updateUser
}