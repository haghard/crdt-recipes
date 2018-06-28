package recipes.users.vversions

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Kill, Props}
import akka.cluster.Cluster
import akka.cluster.ddata.Replicator._
import com.typesafe.config.ConfigFactory
import recipes.Helpers
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
        self ! Kill
      case shardId :: tail =>
        reader ! shardId
        context become await(shardId, tail)
    }
  }

  def await(i: Long, rest: List[Long]): Receive = {
    case usersByShard: Set[User] @unchecked =>
      //expectation
      log.info("All have been updated: " +
        (usersByShard.nonEmpty &&
          usersByShard.forall(_.active == true) &&
          usersByShard.forall(_.online == true)))

      request(rest)
  }

  override def receive: Receive = await(shardIds.head, shardIds.tail)
}

//test:runMain recipes.users.vversions.ReplicatedUsers
object ReplicatedUsers extends App {
  val systemName = "users"
  val commonConfig = ConfigFactory.parseString(
    s"""
      akka {
        cluster {
          roles = [ replicated-users ]
          distributed-data {
            name = replicator
            gossip-interval = 1 s
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

  //All readers and writers share the same replication key

  val writerTimeout = 300.millis
  val readC = ReadLocal /*ReadAll(timeout = timeout)*/ /*ReadMajority(timeout)*/

  val shardIds = List[Long](0)
  
  //In this example we use a single(shared) replication key for all users which means all data is being transferred by gossiping
  //Users id shouldn't overlap because we imply the single writer principle. Thereby that principle
  // we know that VV on "createAt" replica always has the latest version for a particular user value

  node1.actorOf(UserWriter.props("oracle",
    node1.actorOf(ReplicatorWriter.props(node1), "writer-1"), writerTimeout, 0, 10
  ), "oracle-writer")

  node2.actorOf(UserWriter.props("apple",
    node2.actorOf(ReplicatorWriter.props(node2), "writer-2"), writerTimeout, 0, 10
  ), "apple-writer")

  node3.actorOf(UserWriter.props("ms",
    node3.actorOf(ReplicatorWriter.props(node3), "writer-3"), writerTimeout, 0, 10
  ), "ms-writer")

  Helpers.wait(35.second)

  node1.actorOf(UsersByShardView.props(shardIds, node1.actorOf(UsersReader.props(readC), "reader-1")), "view-1")
  node2.actorOf(UsersByShardView.props(shardIds, node2.actorOf(UsersReader.props(readC), "reader-2")), "view-2")
  node3.actorOf(UsersByShardView.props(shardIds, node3.actorOf(UsersReader.props(readC), "reader-3")), "view-3")

  Helpers.wait(5.second)
  println(s"★ ★ ★ ★ ★ ★   Shutdown the cluster after being up for ${System.currentTimeMillis - start} ms  ★ ★ ★ ★ ★ ★")


  node1Cluster.leave(node1Cluster.selfAddress)
  node1.terminate

  node2Cluster.leave(node2Cluster.selfAddress)
  node2.terminate

  node3Cluster.leave(node3Cluster.selfAddress)
  node3.terminate
}