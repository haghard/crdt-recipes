package recipes.users.vversions

import java.util.UUID
import java.util.concurrent.ThreadLocalRandom

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.cluster.Cluster

import scala.concurrent.duration.FiniteDuration

object UserWriter {

  case class CreateUser(userId: String, login: String, isActive: Boolean = false)

  case class ActivateUser(userId: String, isActive: Boolean = true)
  case class LoginUser(userId: String, isLogin: Boolean = true)

  //case class DeleteUser(userId: Long)

  def props(tenant: String, writer: ActorRef, delay: FiniteDuration, startWith: Int, limit: Int) =
    Props(new UserWriter(tenant, writer, delay, startWith, limit))
}

class UserWriter(tenant: String, writer: ActorRef, delay: FiniteDuration, startWith: Long, limit: Int) extends Actor with ActorLogging {

  import UserWriter._

  implicit val ex = context.system.dispatcher
  val cluster = Cluster(context.system)
  val port = cluster.selfAddress.port.get

  var localUserId = startWith
  var userIds: List[Long] = Nil

  override def preStart(): Unit = {
    import scala.concurrent.duration._
    val minDelayMillis = 100
    val delayMillis = ThreadLocalRandom.current().nextLong(minDelayMillis, minDelayMillis + delay.toMillis).millis
    context.setReceiveTimeout(delayMillis)
  }

  override def receive = await()

  def await(): Receive = {
    case akka.actor.ReceiveTimeout =>
      context become add
  }

  def add: Receive = {
    case akka.actor.ReceiveTimeout =>
      if (userIds.size <= limit) {
        val login = UUID.randomUUID.toString.take(6)
        writer ! CreateUser(s"$tenant.$localUserId", login)
        userIds = localUserId :: userIds
        localUserId += 1
      } else {
        val copy = userIds.map(identity)
        context.become(updateActive(copy))
      }
  }

  def updateActive(allUserIds: List[Long]): Receive = {
    case akka.actor.ReceiveTimeout =>
      allUserIds match {
        case userId :: rest =>
          writer ! ActivateUser(s"$tenant.$userId")
          context.become(updateActive(rest))
        case Nil =>
          context.become(updateLogin(userIds))
      }
  }

  def updateLogin(allUserIds: List[Long]): Receive = {
    case akka.actor.ReceiveTimeout =>
      allUserIds match {
        case userId :: rest =>
          writer ! LoginUser(s"$tenant.$userId")
          context.become(updateLogin(rest))
        case Nil =>
          log.info(s"Stop $self")
          context.stop(self)
      }
  }

}
