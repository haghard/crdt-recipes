package recipes

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Kill, Props}
import akka.cluster.Cluster
import akka.cluster.ddata.Replicator._
import com.typesafe.config.ConfigFactory
import recipes.users.{Journal, UsersReader, UsersWriter}
import recipes.users.User

import scala.concurrent.duration._

object UsersByShardView {
  def props(from: Int, to: Int, reader: ActorRef) = Props(new UsersByShardView(from, to, reader))
}

class UsersByShardView(from: Long, to: Long, reader: ActorRef) extends Actor with ActorLogging {
  if (from < to) reader ! from else self ! Kill

  def request(i: Long) = {
    if (i < to) reader ! i else self ! Kill
    context become await(i + 1)
  }

  def await(i: Long): Receive = {
    case usersByShard: Set[User] @unchecked =>
      //log.info(s"shardId:${i - 1} - users: ${usersByShard.toString}")
      log.info("All have been updated: " + usersByShard.forall(_.active == true))
      request(i)
  }

  override def receive: Receive = await(from + 1)
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

  node1.actorOf(Journal.props(1,
    node1.actorOf(UsersWriter.props(1), "writer-1"), writerTimeout, 50, 15
  ), "eh-1")

  node2.actorOf(Journal.props(1,
    node2.actorOf(UsersWriter.props(2), "writer-2"), writerTimeout, 100, 25
  ), "eh-2")

  node3.actorOf(Journal.props(1,
    node3.actorOf(UsersWriter.props(3), "writer-3"), writerTimeout, 150, 30
  ), "eh-3")

  Helpers.wait(40.second)

  node1.actorOf(UsersByShardView.props(1, 4, node1.actorOf(UsersReader.props(readC), "reader-1")), "view-1")
  node2.actorOf(UsersByShardView.props(1, 4, node2.actorOf(UsersReader.props(readC), "reader-2")), "view-2")

  Helpers.wait(6.second)
  println(s"★ ★ ★ ★ ★ ★   Shutdown the cluster after being up for ${System.currentTimeMillis - start} ms    ★ ★ ★ ★ ★ ★")

  node1.terminate
  node1Cluster.leave(node1Cluster.selfAddress)
  node2.terminate
  node2Cluster.leave(node2Cluster.selfAddress)
  node3.terminate
  node3Cluster.leave(node3Cluster.selfAddress)
}
