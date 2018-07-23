package recipes.replication.routing

import akka.actor.ActorSystem
import akka.cluster.Cluster
import com.typesafe.config.ConfigFactory
import recipes.Helpers

import scala.concurrent.duration._

/*
POC of this approach
  https://groups.google.com/forum/#!topic/akka-user/MO-4XhwhAN0 with router group

Sharded replication with distributed data using router group

runMain recipes.replication.routing.Runner

*/
object Runner extends App {
  val systemName = "counts"

  //We have 2 shards on each node and we replicate each message
  val shards = Vector("alpha", "betta", "gamma")

  val configA = ConfigFactory.parseString(
    s"""
       akka {
          cluster {
            roles = [ ${shards(0)}, ${shards(1)} ]
            jmx.multi-mbeans-in-same-jvm = on
          }
          actor.provider = cluster
          remote.artery.enabled = true
          remote.artery.canonical.hostname = 127.0.0.1
       }
      """)

  val configB = ConfigFactory.parseString(
    s"""
       akka {
          cluster {
            roles = [ ${shards(0)}, ${shards(2)} ]
            jmx.multi-mbeans-in-same-jvm = on
          }
          actor.provider = cluster
          remote.artery.enabled = true
          remote.artery.canonical.hostname = 127.0.0.1
       }
      """)

  val configC = ConfigFactory.parseString(
    s"""
       akka {
          cluster {
            roles = [ ${shards(1)}, ${shards(2)} ]
            jmx.multi-mbeans-in-same-jvm = on
          }
          actor.provider = cluster
          remote.artery.enabled = true
          remote.artery.canonical.hostname = 127.0.0.1
       }
      """)

  def portConfig(port: Int) =
    ConfigFactory.parseString(s"akka.remote.artery.canonical.port = $port")

  val node1 = ActorSystem(systemName, portConfig(2550).withFallback(configA))
  val node2 = ActorSystem(systemName, portConfig(2551).withFallback(configB))
  val node3 = ActorSystem(systemName, portConfig(2552).withFallback(configC))

  val node1Cluster = Cluster(node1)
  val node2Cluster = Cluster(node2)
  val node3Cluster = Cluster(node3)

  node1Cluster.join(node1Cluster.selfAddress)
  node2Cluster.join(node1Cluster.selfAddress)
  node3Cluster.join(node1Cluster.selfAddress)

  Helpers.waitForAllNodesUp(node1, node2, node3)

  //
  node1.actorOf(RouterWriter.props(node1, Vector(shards(0), shards(1)), shards(2), 4.seconds, 0), "alpha-writer")
  node2.actorOf(RouterWriter.props(node2, Vector(shards(0), shards(2)), shards(1), 3.seconds, 100), "betta-writer")
  node3.actorOf(RouterWriter.props(node3, Vector(shards(1), shards(2)), shards(0), 1.seconds, 200),"gamma-writer")

  Helpers.wait(40.second)

  node3Cluster.leave(node3Cluster.selfAddress)
  node3.terminate

  Helpers.wait(20.second)

  println("****************** new incarnation of node3 joins the cluster *********************")
  val node31 = ActorSystem(systemName, portConfig(2552).withFallback(configC))
  val node31Cluster = Cluster(node31)
  node31Cluster.join(node1Cluster.selfAddress)
  node31.actorOf(RouterWriter.props(node31, Vector(shards(1), shards(2)), shards(0), 1.seconds, 200),"gamma-writer")

  Helpers.wait(50.second)

  node1Cluster.leave(node1Cluster.selfAddress)
  node1.terminate

  node2Cluster.leave(node2Cluster.selfAddress)
  node2.terminate

  node31Cluster.leave(node31Cluster.selfAddress)
  node31.terminate

}
