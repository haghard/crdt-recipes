package recipes.shardedReplica

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.cluster.Cluster
import akka.cluster.ddata.Replicator._
import akka.cluster.ddata.{PNCounter, PNCounterKey, Replicator, ReplicatorSettings}
import com.typesafe.config.ConfigFactory
import recipes.shardedReplica.ShardWriter.Command

object ShardReplicator {
  def props(system: ActorSystem, shardName: String) =
    Props(new ShardReplicator(system, shardName))
}

class ShardReplicator(system: ActorSystem, shardName: String) extends Actor with ActorLogging {
  //import ShardReplicator._
  val replicatorName = s"replicator-for-$shardName"
  val DataKey = PNCounterKey(shardName + "-counter")

  //import context.dispatcher
  //val tickTask = context.system.scheduler.schedule(interval, interval, self, Tick)

  implicit val cluster = Cluster(system)

  val config = ConfigFactory.parseString(
    s"""
       | name = $replicatorName
       | role = $shardName
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
       |  keys = []
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

  val settings = ReplicatorSettings(config)
  val replicator = system.actorOf(Replicator.props(settings), replicatorName)

  override def preStart(): Unit = {
    replicator ! Subscribe(DataKey, self)
  }

  ///*WriteMajority(2.second)*/
  override def receive = {
    case c: Command =>
      replicator ! Update(DataKey, PNCounter(), WriteLocal)(_ + 1)
    case c @ Changed(DataKey) =>
      val data = c.get(DataKey)
      log.info(s"Gossip change - Shard: {} - Current count:{}", shardName, data.getValue)
  }
}
