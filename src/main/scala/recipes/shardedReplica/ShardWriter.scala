package recipes.shardedReplica

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.cluster.sharding.{ClusterSharding, ClusterShardingSettings, ShardRegion}
import recipes.shardedReplica.ShardWriter.Command

import scala.concurrent.duration.FiniteDuration

object ShardWriter {
  object Tick
  case class Command(i : Int)

  def props(system: ActorSystem,/* reps: Map[String, ActorRef],*/
    shards: Vector[String],
    interval: FiniteDuration, startWith: Int) =
    Props(new ShardWriter(system, shards, interval, startWith))
}

class ShardWriter(system: ActorSystem,   /*reps: Map[String, ActorRef],*/
  shards: Vector[String],
  interval: FiniteDuration, startWith: Int) extends Actor with ActorLogging {
  import ShardWriter._

  import system.dispatcher
  system.scheduler.schedule(interval, interval, self, Tick)

  val numberOfShards = shards.size

  def create(role: String/*, replicator: ActorRef*/) = {

    val EntityId: ShardRegion.ExtractEntityId = {
      case msg@Command(id) ⇒ ((id % numberOfShards).toString, msg)
    }

    val ShardId: ShardRegion.ExtractShardId = {
      case Command(id) ⇒ (id % numberOfShards).toString
    }

    val replicator = system.actorOf(ShardReplicator.props(system, role), role)
    //val b1 = node1.actorOf(ShardReplicator.props(node1, shards(1)), shards(1))

    //cluster sharding gives you one actor per entity,
    (role, ClusterSharding(system).start(
      typeName = s"shard-region-to-$role",
      entityProps = ReplicatorEntity.props(replicator),
      settings = ClusterShardingSettings(system).withRole(role),
      extractEntityId = EntityId,
      extractShardId = ShardId
    ))
  }

  val shards0 = shards.map { shardName => create(shardName) }
  val actors = shards0.map(_._2)
  val roles = shards0.map(_._1)

  val routes =  actors.zipWithIndex.foldLeft(Map.empty[Int, ActorRef]) { (acc, c) =>
    acc + (c._2 -> c._1)
  }

  //val routes = Map(0 -> actors(0), 1 -> actors(1))

  var i = startWith
  var cursor = 0

  //val regions = reps.keySet.map(role => ClusterSharding(system).shardRegion(s"shard-region-to-${role}"))

  override def receive = active(0)

  def active(index: Int): Receive = {
    case Tick =>
      val ind = index % roles.size
      val role = roles(ind)
      val shardRegion = routes(ind)
      log.info("writer pick {} for message {}", role, i)
      shardRegion ! Command(i)
      i = i + 1
      if(i % numberOfShards == 0)
        context.become(active(index + 1))
  }
}

/*
[akka://users@127.0.0.1:2550/system/sharding/shard-region-to-shard-A/0/0] ReplicatorEntity for Actor[akka://users/user/shard-A#2137932057]
[akka://users@127.0.0.1:2551/system/sharding/shard-region-to-shard-A/1/1] ReplicatorEntity for Actor[akka://users/user/shard-A#726878514]

[akka://users@127.0.0.1:2550/system/sharding/shard-region-to-shard-B/0/0] ReplicatorEntity for Actor[akka://users/user/shard-B#1206127316]
[akka://users@127.0.0.1:2551/system/sharding/shard-region-to-shard-B/1/1] ReplicatorEntity for Actor[akka://users/user/shard-B#946072560]

 */
object ReplicatorEntity {
  def props(replicator: ActorRef) = Props(new ReplicatorEntity(replicator))
}

class ReplicatorEntity(replicator: ActorRef) extends Actor with ActorLogging {
  override def preStart = {
    log.info("Allocate ReplicatorEntity for {}", replicator)
  }

  override def receive = {
    case msg: Command ⇒
      replicator forward msg
  }
}