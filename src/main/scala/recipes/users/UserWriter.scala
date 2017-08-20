package recipes.users

import akka.cluster.Cluster
import java.util.concurrent.ThreadLocalRandom
import scala.concurrent.duration.FiniteDuration
import akka.actor.{ Actor, ActorLogging, ActorRef, Props }

object UserWriter {

  case class CreateUser(userId: Long, login: String, isActive: Boolean)

  case class UpdateUser(userId: Long, login: String, isActive: Boolean)

  case class DeleteUser(userId: Long)

  def props(shardId: Long, updater: ActorRef, delay: FiniteDuration, startWith: Int, limit: Int) =
    Props(new UserWriter(shardId, updater, delay, startWith, limit))
}

class UserWriter(shardId: Long, updater: ActorRef, delay: FiniteDuration, startWith: Long, limit: Int) extends Actor with ActorLogging {
  import UserWriter._

  implicit val ex = context.system.dispatcher
  val cluster = Cluster(context.system)
  val port = cluster.selfAddress.port.get

  var localUserId = startWith
  var userIds: List[Long] = Nil

  override def preStart(): Unit = {
    import scala.concurrent.duration._
    val mil = ThreadLocalRandom.current().nextLong(100, delay.toMillis)
    context.setReceiveTimeout(mil.millis)
  }

  override def receive = await()

  def await(): Receive = {
    case akka.actor.ReceiveTimeout =>
      context become add
  }

  def add: Receive = {
    case akka.actor.ReceiveTimeout =>
      if (userIds.size <= limit) {
        updater ! CreateUser(localUserId, s"login-$localUserId", false)
        userIds = localUserId :: userIds
        localUserId += 1
      } else {
        context.become(update)
        //context.become(delete)
        //context.stop(self)
      }
  }

  def update: Receive = {
    case akka.actor.ReceiveTimeout =>
      userIds match {
        case userId :: rest =>
          updater ! UpdateUser(userId, s"new-login", true)
          //log.info(s"UpdateUser for shard: ${shardId} with id $userId")
          userIds = rest
        case Nil =>
          log.info(s"Stop user-writer for shardId:$shardId")
          context.stop(self)
      }
  }
}
