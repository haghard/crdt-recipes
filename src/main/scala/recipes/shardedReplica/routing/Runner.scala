package recipes.shardedReplica.routing

import akka.actor.ActorSystem
import akka.cluster.Cluster
import com.typesafe.config.ConfigFactory
import recipes.Helpers

import scala.concurrent.duration._

/*

POC of this approach
  https://groups.google.com/forum/#!topic/akka-user/MO-4XhwhAN0 with router group

runMain recipes.shardedReplica.routing.Runner

*/

//Sharded replication with distributed data using router group
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

  /*def conf(shardName: Vector[String]) =
    ConfigFactory.parseString(
    s"""
      |
      |akka {
      |  cluster {
      |    roles = [ ${shardName.mkString(",")} ]
      |    jmx.multi-mbeans-in-same-jvm = on
      |    #sharding.state-store-mode = persistence
      |
      |    distributed-data {
      |     name = ${shardName}-replicator
      |     role = $shardName
      |     gossip-interval = 1 s
      |     use-dispatcher = ""
      |     notify-subscribers-interval = 500 ms
      |     max-delta-elements = 1000
      |     pruning-interval = 120 s
      |     max-pruning-dissemination = 300 s
      |     pruning-marker-time-to-live = 6 h
      |     serializer-cache-time-to-live = 10s
      |
      |     delta-crdt {
      |	      enabled = on
      |	      max-delta-size = 1000
      |     }
      |
      |     durable {
      |	      keys = []
      |	      pruning-marker-time-to-live = 10 d
      |	      store-actor-class = akka.cluster.ddata.LmdbDurableStore
      |	      use-dispatcher = akka.cluster.distributed-data.durable.pinned-store
      |	      pinned-store {
      |	        executor = thread-pool-executor
      |	        type = PinnedDispatcher
      |       }
      |     }
      |
      |    }
      |  }
      |
      |  actor.provider = cluster
      |  remote.artery.enabled = true
      |  remote.artery.canonical.hostname = 127.0.0.1
      |}
    """.stripMargin)*/

  def portConfig(port: Int) = ConfigFactory.parseString(s"akka.remote.artery.canonical.port = $port")

  val node1 = ActorSystem(systemName, portConfig(2550).withFallback(configA))
  val node2 = ActorSystem(systemName, portConfig(2551).withFallback(configB))
  val node3 = ActorSystem(systemName, portConfig(2553).withFallback(configC))

  val node1Cluster = Cluster(node1)
  val node2Cluster = Cluster(node2)
  val node3Cluster = Cluster(node3)

  node1Cluster.join(node1Cluster.selfAddress)
  node2Cluster.join(node1Cluster.selfAddress)
  node3Cluster.join(node1Cluster.selfAddress)

  Helpers.waitForAllNodesUp(node1, node2, node3)

  node1.actorOf(RouterWriter.props(node1, Vector(shards(0), shards(1)), shards(2), 2.seconds, 0), "alpha-writer") //
  node2.actorOf(RouterWriter.props(node2, Vector(shards(0), shards(2)), shards(1), 3.seconds, 100), "betta-writer")
  node3.actorOf(RouterWriter.props(node3, Vector(shards(1), shards(2)), shards(0), 4.seconds, 200),"gamma-writer")

  Helpers.wait(30.second)

  node3Cluster.leave(node3Cluster.selfAddress)
  node3.terminate

  Helpers.wait(30.second)

  node1Cluster.leave(node1Cluster.selfAddress)
  node1.terminate

  node2Cluster.leave(node2Cluster.selfAddress)
  node2.terminate

}
