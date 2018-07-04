package recipes.shardedReplica

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.cluster.Cluster
import akka.cluster.ddata.Replicator._
import akka.cluster.ddata._
import com.typesafe.config.ConfigFactory
import recipes.shardedReplica.sharding.ShardWriter.Command

object Replicator2 {
  def props(system: ActorSystem, role: String) = Props(new Replicator2(system, role))
}

class Replicator2(system: ActorSystem, shard: String) extends Actor with ActorLogging {
  val replicatorName = s"$shard-replicator"

  val DataKey = GSetKey[Int](shard + "key")

  implicit val cluster = Cluster(system)

  val config = ConfigFactory.parseString(
    s"""
       | name = $replicatorName
       | role = $shard
       | gossip-interval = 1 s
       | use-dispatcher = ""
       | notify-subscribers-interval = 500 ms
       | max-delta-elements = 1000
       | pruning-interval = 120 s
       | max-pruning-dissemination = 300 s
       | pruning-marker-time-to-live = 6 h
       | serializer-cache-time-to-live = 10s
       | delta-crdt {
       |   enabled = on
       |   max-delta-size = 1000
       | }
       |
       | durable {
       |  keys = ["*"]
       |  pruning-marker-time-to-live = 10 d
       |  store-actor-class = akka.cluster.ddata.LmdbDurableStore
       |  use-dispatcher = akka.cluster.distributed-data.durable.pinned-store
       |  pinned-store {
       |    executor = thread-pool-executor
       |    type = PinnedDispatcher
       |  }
       |
       |  lmdb {
       |    dir = "ddata"
       |    map-size = 100 MiB
       |    write-behind-interval = off
       |  }
       | }
      """.stripMargin)

  val replicator = system.actorOf(Replicator.props(ReplicatorSettings(config)), replicatorName)

  var input = List.empty[Int]

  override def preStart(): Unit = {
    log.info("Start replicator {} ", replicatorName)
    replicator ! Subscribe(DataKey, self)
  }

  ///*WriteMajority(2.second)*/
  override def receive = {
    case c: Command =>
      //log.info(s"accept command ${c.i}")
      input = c.i :: input
      replicator ! Update(DataKey, GSet.empty[Int], WriteLocal)(_.+(c.i))
    case c @ Changed(DataKey) =>
      val numbers = c.get(DataKey).elements
      log.info(s"Change on shard: {} - {}", shard, numbers.mkString(","))
  }
}
