package recipes.shardedReplica.sharding

import akka.actor.ActorSystem
import akka.cluster.Cluster
import com.typesafe.config.ConfigFactory
import recipes.Helpers

import scala.concurrent.duration._

//runMain recipes.shardedReplica.sharding.Runner

/*

POC of this approach
  https://groups.google.com/forum/#!topic/akka-user/MO-4XhwhAN0 with sharding

  You can use Cluster Sharding and DData with roles. So, let's say that you go with 10 roles, 10,000 entities in each role.
  You would then start Replicators on the nodes with corresponding roles.
  You would also start Sharding on the nodes with corresponding roles.
  On a node that doesn't have the a role you would start a sharding proxy for such role.

  When you want to send a message to an entity you first need to decide which role to use for that message.
  Can be simple hashCode modulo algorithm.
  Then you delegate the message to the corresponding Sharding region or proxy actor.

  You have defined the Props for the entities and there you pass in the Replicator corresponding to the role that the entity
  belongs to, i.e. the entity takes the right Replicator ActorRef as constructor parameter.

  If you don't need the strict guarantees of "only one entity" that Cluster Sharding provides, and prefer better availability in
  case of network partitions, you could use a consistent hashing group router instead of Cluster Sharding.
  You would have one router per role, and decide router similar as above.
  Then the entities (routees of the router) would have to subscribe to changes
  from DData to get notified of when a peer entity has changed something, since you can have more than one alive at the same time.


  1. How are Replicators tied to node roles?

  Replicator.props takes ReplicatorSettings, which contains a role property.

  Can I start more than 1 Replicator on a node?
  Yes, just start it as an ordinary actor. Make sure that you use the same actor name on other nodes that it should interact with.

  If so, can I start only as many Replicators as the roles this node has?

  Yes, that was my idea

  2. If a node has K roles, does it mean that its K replicators gossip independently of each other?

  Yes

  3. In the last scenario --- one consistent hashing group router per role --- why do routees subscribe to changes from DData? Shouldn't DData be replicated across all nodes with role_i? If so, they can simply read the data if they are on the node with the right role.

  Yes they can read instead, but then you would need to know when to read. Perhaps you do that for each request, that would also work.
*/
object Runner extends App {
  val systemName = "counts"

  //We have 2 shards on each node and we replicate each shard 3 times
  val shards = Vector("shard-A", "shard-B")

  val commonConfig = ConfigFactory.parseString(
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
      """
  )

  //https://github.com/facebook/rocksdb/blob/master/java/samples/src/main/java/RocksDBColumnFamilySample.java
  //https://github.com/facebook/rocksdb/blob/master/java/samples/src/main/java/RocksDBSample.java
  //org.rocksdb.RocksDB.loadLibrary()

  def portConfig(port: Int) = ConfigFactory.parseString(s"akka.remote.artery.canonical.port = $port")

  val node1 = ActorSystem(systemName, portConfig(2550).withFallback(commonConfig))
  val node2 = ActorSystem(systemName, portConfig(2551).withFallback(commonConfig))

  //val node3 = ActorSystem(systemName, portConfig(2552).withFallback(commonConfig))
  //val client = ActorSystem(systemName, portConfig(2553).withFallback(routerConfig))


  val node1Cluster = Cluster(node1)
  val node2Cluster = Cluster(node2)
  //val node3Cluster = Cluster(node3)
  //val node4Cluster = Cluster(client)

  node1Cluster.join(node1Cluster.selfAddress)
  node2Cluster.join(node1Cluster.selfAddress)
  //node3Cluster.join(node1Cluster.selfAddress)
  //node4Cluster.join(node1Cluster.selfAddress)

  Helpers.waitForAllNodesUp(node1, node2)

 /*
 Allocation
 [akka://counts@127.0.0.1:2550/system/sharding/shard-region-to-shard-A/shard-A/0] Allocate ReplicatorEntity
 [akka://counts@127.0.0.1:2550/system/sharding/shard-region-to-shard-B/shard-A/0] Allocate ReplicatorEntity
 [akka://counts@127.0.0.1:2551/system/sharding/shard-region-to-shard-A/shard-B/1] Allocate ReplicatorEntity
 [akka://counts@127.0.0.1:2551/system/sharding/shard-region-to-shard-B/shard-B/1] Allocate ReplicatorEntity

 Distribution
 [akka://counts@127.0.0.1:2550/system/sharding/shard-region-to-shard-A/shard-A/0] 0,20,12,16,104,8,4,100
 [akka://counts@127.0.0.1:2550/system/sharding/shard-region-to-shard-B/shard-A/0] 10,14,106,6,102,2,22,18
 [akka://counts@127.0.0.1:2551/system/sharding/shard-region-to-shard-A/shard-B/1] 101,5,1,21,9,13,105,17
 [akka://counts@127.0.0.1:2551/system/sharding/shard-region-to-shard-B/shard-B/1] 7,103,3,11,23,19,107,15
 */

  //val a2 = node2.actorOf(ShardReplicator.props(node2, shards(0)), shards(0))
  //val b2 = node2.actorOf(ShardReplicator.props(node2, shards(1)), shards(1))
  node1.actorOf(ShardWriter.props(node1, shards, 2.seconds, 0))

  //val a3 = node3.actorOf(ShardReplicator.props(node3, shards(0), 6.seconds), shards(0))
  //val b3 = node3.actorOf(ShardReplicator.props(node3, shards(1), 5.seconds), shards(1))
  node2.actorOf(ShardWriter.props(node2, shards, 6.seconds, 100))


  Helpers.wait(50.second)

  node1Cluster.leave(node1Cluster.selfAddress)
  node1.terminate

  node2Cluster.leave(node2Cluster.selfAddress)
  node2.terminate
}