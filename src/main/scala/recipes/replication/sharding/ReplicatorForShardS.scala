package recipes.replication.sharding

import java.util.UUID

import akka.actor.{Actor, ActorLogging, Props}
import akka.cluster.Cluster
import akka.cluster.ddata.Replicator._
import akka.cluster.ddata._
import com.typesafe.config.{Config, ConfigFactory}
import recipes.replication.sharding.ShardWriter.Command

object ReplicatorForShardS {
  def props(role: String) = Props(new ReplicatorForShardS(role))
}

class ReplicatorForShardS(shardName: String) extends Actor with ActorLogging {
  val DataKey = GSetKey[Int](shardName + "-key")
  val replicatorName = s"$shardName-replicator"

  implicit val cluster = Cluster(context.system)

  //val config = context.system.settings.config.getConfig("akka.cluster.distributed-data")

  val config: Config = ConfigFactory.parseString(
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
       |  keys = ["*"]
       |  pruning-marker-time-to-live = 10 d
       |  store-actor-class = akka.cluster.ddata.RocksDurableStore
       |  use-dispatcher = akka.cluster.distributed-data.durable.pinned-store
       |  pinned-store {
       |    executor = thread-pool-executor
       |    type = PinnedDispatcher
       |  }
       |
       |  rocks {
       |    dir = "ddata"
       |  }
       |
       |  lmdb {
       |    dir = "ddata"
       |    map-size = 100 MiB
       |    write-behind-interval = off
       |  }
       | }
      """.stripMargin)


  val dStorageClass = config.getString("durable.store-actor-class")
  //val replicatorName = config.getString("name")

  val akkaReplicator =
    context.actorOf(Replicator.props(ReplicatorSettings(config)), replicatorName)

  override def preStart(): Unit = {
    log.info("******************  Start replicator {} backed by {}", replicatorName, dStorageClass)
    akkaReplicator ! Subscribe(DataKey, self)
  }

  ///*WriteMajority(2.second)*/
  override def receive = {
    case c: Command =>
      //input = c.i :: input
      //log.info(s"replic input {}", input.mkString(","))
      val correlationId = UUID.randomUUID.toString
      akkaReplicator ! Update(DataKey, GSet.empty[Int], WriteLocal, Some(correlationId))(_.+(c.i))
    case UpdateSuccess(DataKey, Some(correlationId)) =>

    case UpdateTimeout(DataKey, Some(correlationId)) =>

    case c @ Changed(DataKey) =>
      val numbers = c.get(DataKey).elements
      log.info(s"Change on shard: {} - {}", shardName, numbers.toSeq.sorted.mkString(","))
     case ModifyFailure(DataKey, error, cause, Some(correlationId)) =>
       log.error(s"ModifyFailure on shard: {} for key {} {}", shardName, DataKey, correlationId)
  }
}