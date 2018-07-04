package recipes.users.single

import scala.concurrent.duration._
import akka.actor.ActorSystem
import akka.cluster.Cluster
import akka.cluster.ddata.Replicator.ReadLocal
import com.typesafe.config.ConfigFactory
import recipes.Helpers
import recipes.users.UsersQuery

//test:runMain recipes.users.single.ReplicatedUsers
/**
 *
 * In this example we use one global key for replication, therefore
 * despite this fact that we also use the single writer principle(each writer/reader writes/reads users in its own tenant)
 * the next things are true:
 *  a) conflicts are possible and we resolve them at `VersionedUsers.merge`.
 *  The merge strategy always the same - we pick user from the replica that has highest VV for node where this user was created.
 *  b) all users are being transferred by gossiping (a single replication message includes them all)
 */
object ReplicatedUsers extends App {
  val systemName = "users"

  val arteryCommonConfig = ConfigFactory.parseString(
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

  def portConfig(port: Int) =
    ConfigFactory.parseString(s"akka.remote.artery.canonical.port = $port")

  val node1 = ActorSystem(systemName, portConfig(2550).withFallback(arteryCommonConfig))
  val node2 = ActorSystem(systemName, portConfig(2551).withFallback(arteryCommonConfig))
  val node3 = ActorSystem(systemName, portConfig(2552).withFallback(arteryCommonConfig))

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

  val writerTimeout = 300.millis
  val readC = ReadLocal

  val tenants = List[String]("oracle", "apple", "ms")

  node1.actorOf(UsersWriter.props(tenants(0),
    node1.actorOf(UsersReplicator.props(tenants(0), node1), s"${tenants(0)}-replicator"), writerTimeout, 0, 10
  ), s"${tenants(0)}-writer")

  node2.actorOf(UsersWriter.props(tenants(1),
    node2.actorOf(UsersReplicator.props(tenants(1), node2), s"${tenants(1)}-replicator"), writerTimeout, 0, 10
  ), s"${tenants(1)}-writer")

  node3.actorOf(UsersWriter.props(tenants(2),
    node3.actorOf(UsersReplicator.props(tenants(2), node3), s"${tenants(2)}-replicator"), writerTimeout, 0, 10
  ), s"${tenants(2)}-writer")

  Helpers.wait(35.second)

  node1.actorOf(UsersQuery.props(tenants, node1.actorOf(UsersReader.props(readC), "reader-1")), "view-1")
  node2.actorOf(UsersQuery.props(tenants, node2.actorOf(UsersReader.props(readC), "reader-2")), "view-2")
  node3.actorOf(UsersQuery.props(tenants, node3.actorOf(UsersReader.props(readC), "reader-3")), "view-3")

  Helpers.wait(10.second)
  println(s"★ ★ ★ ★ ★ ★   Shutdown the cluster after being up for ${System.currentTimeMillis - start} ms  ★ ★ ★ ★ ★ ★")


  node1Cluster.leave(node1Cluster.selfAddress)
  node1.terminate

  node2Cluster.leave(node2Cluster.selfAddress)
  node2.terminate

  node3Cluster.leave(node3Cluster.selfAddress)
  node3.terminate
}