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
  On a node that doesn't have the role you would start a sharding proxy for such role.

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

  3. In the last scenario --- one consistent hashing group router per role --- why do routees subscribe to changes from DData?
  Shouldn't DData be replicated across all nodes with role_i?
  If so, they can simply read the data if they are on the node with the right role.

  Yes they can read instead, but then you would need to know when to read. Perhaps you do that for each request, that would also work.
*/

//Sharded replication with distributed data
object Runner extends App {
  val systemName = "counter"

  //We have 2 shards on each node and we replicate each shard 3 times
  val allShards = Vector("alpha", "beta", "gamma")

  /*val configA = ConfigFactory.parseString(
    s"""
       akka {
          cluster {
            roles = [ ${allShards(0)} ]
            jmx.multi-mbeans-in-same-jvm = on
            # sharding.state-store-mode = ddata persistence
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
            roles = [ ${allShards(1)} ]
            jmx.multi-mbeans-in-same-jvm = on
            #sharding.state-store-mode = persistence
          }

          actor.provider = cluster
          remote.artery.enabled = true
          remote.artery.canonical.hostname = 127.0.0.1
       }
      """)*/

  //RocksDurableStore
  //LmdbDurableStore
  def conf(shardName: String) =
    ConfigFactory.parseString(
    s"""
      |
      |akka {
      |  cluster {
      |    roles = [ $shardName ]
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
    """.stripMargin)

  def conf0(shardName: String) =
      ConfigFactory.parseString(
      s"""
        |
        |akka {
        |  cluster {
        |    roles = [ $shardName ]
        |    jmx.multi-mbeans-in-same-jvm = on
        |    #sharding.state-store-mode = persistence
        |  }
        |
        |
        |  actor.provider = cluster
        |  remote.artery.enabled = true
        |  remote.artery.canonical.hostname = 127.0.0.1
        |}
      """.stripMargin)

  def portConfig(port: Int) =
    ConfigFactory.parseString(s"akka.remote.artery.canonical.port = $port")

  val node1 = ActorSystem(systemName, portConfig(2550).withFallback(conf(allShards(0))))
  val node2 = ActorSystem(systemName, portConfig(2551).withFallback(conf(allShards(0))))

  val node3 = ActorSystem(systemName, portConfig(2552).withFallback(conf(allShards(1))))
  val node4 = ActorSystem(systemName, portConfig(2553).withFallback(conf(allShards(1))))

  val node1Cluster = Cluster(node1)
  val node2Cluster = Cluster(node2)
  val node3Cluster = Cluster(node3)
  val node4Cluster = Cluster(node4)

  node1Cluster.join(node1Cluster.selfAddress)
  node2Cluster.join(node1Cluster.selfAddress)
  node3Cluster.join(node1Cluster.selfAddress)
  node4Cluster.join(node1Cluster.selfAddress)

  Helpers.waitForAllNodesUp(node1, node2, node3, node4)

  node1.actorOf(ShardWriter.props(node1, allShards(0), allShards(1), 5.seconds, 0), "alpha0-writer")
  node2.actorOf(ShardWriter.props(node2, allShards(0), allShards(1), 5.seconds, 100), "alpha1-writer")

  node3.actorOf(ShardWriter.props(node3, allShards(1), allShards(0), 6.seconds, 200), "betta0-writer")
  node4.actorOf(ShardWriter.props(node4, allShards(1), allShards(0), 6.seconds, 300), "betta1-writer")

  Helpers.wait(50.second)

  node2Cluster.leave(node2Cluster.selfAddress)
  node2.terminate

  Helpers.wait(30.second)

  node3Cluster.leave(node3Cluster.selfAddress)
  node3.terminate

  node4Cluster.leave(node4Cluster.selfAddress)
  node4.terminate

  node1Cluster.leave(node1Cluster.selfAddress)
  node1.terminate
}