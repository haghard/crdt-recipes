package recipes.replication.routing

import java.util.UUID

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.cluster.Cluster
import akka.cluster.ddata.Replicator._
import akka.cluster.ddata._
import com.typesafe.config.{Config, ConfigFactory}
import recipes.replication.sharding.ShardWriter.Command

object ReplicatorForShardR {
  def props(system: ActorSystem, role: String) = Props(new ReplicatorForShardR(system, role))
}

class ReplicatorForShardR(system: ActorSystem, shardName: String) extends Actor with ActorLogging {
  val replicatorName = s"$shardName-replicator"

  val DataKey = GSetKey[Int](shardName + "-key")

  implicit val cluster = Cluster(context.system)

  //LmdbDurableStore
  //RocksDurableStore
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

  val akkaReplicator =
    system.actorOf(Replicator.props(ReplicatorSettings(config)), replicatorName)

  //var input = List.empty[Int]

  override def preStart(): Unit = {
    log.info("******************  Start replicator {} backed by {}", replicatorName, dStorageClass)
    akkaReplicator ! Subscribe(DataKey, self)
  }

  ///*WriteMajority(2.second)*/
  override def receive = {
    case c: Command =>
      //input = c.i :: input
      //log.info(s"replic input {}", input.mkString(","))

      /*if(DataKey._id == "gamma-key")
        log.info(s"accept a command {} ", c.i)
      */

      val correlationId = UUID.randomUUID.toString
      akkaReplicator ! Update(DataKey, GSet.empty[Int], WriteLocal, Some(correlationId))(_.+(c.i))
    case UpdateSuccess(DataKey, Some(correlationId)) =>
       
      //replyTo ! "ack"
    case UpdateTimeout(DataKey, Some(correlationId)) =>
       //replyTo ! "nack"
    case c @ Changed(DataKey) =>
      val numbers = c.get(DataKey).elements
      //
      log.info(s"Change on shard: {} - {}", shardName, numbers.toSeq.sorted.mkString(","))
     case ModifyFailure(DataKey, error, cause, Some(correlationId)) =>
       log.error(s"ModifyFailure on shard: {} for key {} {}", shardName, DataKey, correlationId)
  }
}