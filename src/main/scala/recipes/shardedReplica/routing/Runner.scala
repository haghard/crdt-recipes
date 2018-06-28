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
  val shards = Vector("shard-A", "shard-B", "shard-C")

  val commonConfig = ConfigFactory.parseString(
    s"""
       akka {
          cluster {
            roles = [ ${shards(0)}, ${shards(1)}, ${shards(2)} ]
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

  node1Cluster.join(node1Cluster.selfAddress)
  node2Cluster.join(node1Cluster.selfAddress)
  node3Cluster.join(node1Cluster.selfAddress)

  Helpers.waitForAllNodesUp(node1, node2, node3)

  node1.actorOf(ConsistentHashingRouterWriter.props(node1, shards, 2.seconds, 0))
  node2.actorOf(ConsistentHashingRouterWriter.props(node2, shards, 6.seconds, 100))
  node3.actorOf(ConsistentHashingRouterWriter.props(node3, shards, 4.seconds, 1000))

  Helpers.wait(50.second)

  node1Cluster.leave(node1Cluster.selfAddress)
  node1.terminate

  node2Cluster.leave(node2Cluster.selfAddress)
  node2.terminate

  node3Cluster.leave(node3Cluster.selfAddress)
  node3.terminate

  /*
  Allocation
  [akka://counts@127.0.0.1:2550/user/routeeReplicator-shard-A] Allocate RouteeReplicator
  [akka://counts@127.0.0.1:2550/user/routeeReplicator-shard-B] Allocate RouteeReplicator
  [akka://counts@127.0.0.1:2551/user/routeeReplicator-shard-A] Allocate RouteeReplicator
  [akka://counts@127.0.0.1:2551/user/routeeReplicator-shard-B] Allocate RouteeReplicator

  Distribution
  [akka://counts@127.0.0.1:2550/user/routeeReplicator-shard-A] 20,21,13,105,100
  [akka://counts@127.0.0.1:2551/user/routeeReplicator-shard-A] 101,0,5,1,9,17,12,16,104,8,4
  [akka://counts@127.0.0.1:2550/user/routeeReplicator-shard-B] 106,2,22,103,18,11,19,107,15
  [akka://counts@127.0.0.1:2551/user/routeeReplicator-shard-B] 10,14,6,102,7,3,23
  */
}
