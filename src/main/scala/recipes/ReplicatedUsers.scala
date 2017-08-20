package recipes

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Kill, Props}
import akka.cluster.Cluster
import akka.cluster.ddata.Replicator._
import com.typesafe.config.ConfigFactory
import recipes.users.{UserWriter, UsersReader, ReplicatorWriter}
import recipes.users.User

import scala.concurrent.duration._

object UsersByShardView {
  def props(shardIds: List[Long], reader: ActorRef) = Props(new UsersByShardView(shardIds, reader))
}

class UsersByShardView(shardIds: List[Long], reader: ActorRef) extends Actor with ActorLogging {
  if (shardIds.isEmpty) self ! Kill else reader ! shardIds.head

  def request(rest: List[Long]) = {
    rest match {
      case Nil =>
        log.info("View kills itself")
        self ! Kill
      case shardId  ::  tail =>
        reader !  shardId
        context become await(shardId, tail)
    }
  }

  def await(i: Long, rest: List[Long]): Receive = {
    case usersByShard: Set[User] @unchecked =>
      //log.info(s"shardId:${i - 1} - users: ${usersByShard.toString}")
      log.info("All have been updated: " + usersByShard.forall(_.active == true))
      request(rest)
  }

  override def receive: Receive = await(shardIds.head, shardIds.tail)
}

//test:runMain recipes.ReplicatedUsers
object ReplicatedUsers extends App {
  val systemName = "users"

  val commonConfig = ConfigFactory.parseString(
    s"""
      akka {
        cluster {
          # seed-nodes = [ "akka://${systemName}@127.0.0.1:2550", "akka://${systemName}@127.0.0.1:2551" ]

          distributed-data {
            name = replicator
            gossip-interval = 3 s
          }

          jmx.multi-mbeans-in-same-jvm = on
        }

        actor.provider = cluster
        remote.artery.enabled = true
        remote.artery.canonical.hostname = 127.0.0.1
      }
    """
  )

  def portConfig(port: Int) = ConfigFactory.parseString(s"akka.remote.artery.canonical.port = $port")

  val node1 = ActorSystem(systemName, portConfig(2550).withFallback(commonConfig))
  val node2 = ActorSystem(systemName, portConfig(2551).withFallback(commonConfig))
  val node3 = ActorSystem(systemName, portConfig(2552).withFallback(commonConfig))

  val node1Cluster = Cluster(node1)
  val node2Cluster = Cluster(node2)
  val node3Cluster = Cluster(node3)

  // joins itself to form cluster
  node1Cluster.join(node1Cluster.selfAddress)

  // joins the cluster through the one node in the cluster
  node2Cluster.join(node1Cluster.selfAddress)

  // subsequent nodes can join through any node that is already in the cluster
  node3Cluster.join(node2Cluster.selfAddress)

  Helpers.waitForAllNodesUp(node1, node2, node3)
  val start = System.currentTimeMillis
  println(s"★ ★ ★ ★ ★ ★   Cluster has been formed   ★ ★ ★ ★ ★ ★")

  val writerTimeout = 500.millis
  val readC = ReadLocal /*ReadAll(timeout = timeout)*/ /*ReadMajority(timeout)*/

  val shardIds = List[Long](2l, 7l, 16l)

  node1.actorOf(UserWriter.props(shardIds(0),
    node1.actorOf(ReplicatorWriter.props(shardIds(0)), "writer-1"), writerTimeout, 0, 10
  ), s"writer-${shardIds(0)}")

  node2.actorOf(UserWriter.props(shardIds(1),
    node2.actorOf(ReplicatorWriter.props(shardIds(1)), "writer-2"), writerTimeout, 0, 25
  ), s"writer-${shardIds(1)}")

  node3.actorOf(UserWriter.props(shardIds(2),
    node3.actorOf(ReplicatorWriter.props(shardIds(2)), "writer-3"), writerTimeout, 0, 30
  ), s"writer-${shardIds(2)}")

  Helpers.wait(35.second)

  node1.actorOf(UsersByShardView.props(shardIds, node1.actorOf(UsersReader.props(readC), "reader-1")), "view-1")
  node2.actorOf(UsersByShardView.props(shardIds, node2.actorOf(UsersReader.props(readC), "reader-2")), "view-2")

  Helpers.wait(6.second)
  println(s"★ ★ ★ ★ ★ ★   Shutdown the cluster after being up for ${System.currentTimeMillis - start} ms    ★ ★ ★ ★ ★ ★")

  node1.terminate
  node1Cluster.leave(node1Cluster.selfAddress)
  node2.terminate
  node2Cluster.leave(node2Cluster.selfAddress)
  node3.terminate
  node3Cluster.leave(node3Cluster.selfAddress)
}
