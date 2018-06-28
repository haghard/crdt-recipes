package recipes.shardedReplica.sharding

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.cluster.sharding.{ClusterSharding, ClusterShardingSettings, ShardRegion}
import recipes.shardedReplica.ReplicatorForRole
import recipes.shardedReplica.sharding.ShardWriter.Command

import scala.concurrent.duration.FiniteDuration

object ShardWriter {

  object Tick

  case class Command(i: Int)

  def props(system: ActorSystem, shards: Vector[String], interval: FiniteDuration, startWith: Int) =
    Props(new ShardWriter(system, shards, interval, startWith))
}

class ShardWriter(system: ActorSystem, shards: Vector[String], interval: FiniteDuration, startWith: Int)
  extends Actor with ActorLogging {

  import ShardWriter._
  import system.dispatcher

  system.scheduler.schedule(interval, interval, self, Tick)

  val numberOfShards = shards.size

  def create(role: String) = {

    def entityId: ShardRegion.ExtractEntityId = {
      case msg @ Command(id) ⇒ ((id % numberOfShards).toString, msg)
    }

    def shardId: ShardRegion.ExtractShardId = {
      case Command(id) ⇒ shards(id % numberOfShards)
      case ShardRegion.StartEntity(id) => shards(id.hashCode % numberOfShards)
    }

    //ShardCoordinator.LeastShardAllocationStrategy

    val replicator = system.actorOf(ReplicatorForRole.props(system, role), role)

    //cluster-sharding gives you one actor per entity
    (role, ClusterSharding(system).start(
      typeName = s"shard-region-to-$role",
      entityProps = ReplicatorEntity.props(replicator),
      settings = ClusterShardingSettings(system).withRole(role).withRememberEntities(true),
      extractEntityId = entityId,
      extractShardId = shardId
    ))
  }

  val shards0 = shards.map(create(_))
  val shardRegions = shards0.map(_._2)
  val roles = shards0.map(_._1)

  val routes = shardRegions.zipWithIndex.foldLeft(Map.empty[Int, ActorRef]) { (acc, c) =>
    acc + (c._2 -> c._1)
  }

  var i = startWith
  var cursor = 0

  override def receive = active(0)

  def active(index: Int): Receive = {
    case Tick =>
      val seqNumber = index % roles.size
      val shard = roles(seqNumber)
      val shardRegion = routes(seqNumber)
      log.info("writer pick {} for message {}", shard, i)
      shardRegion ! Command(i)
      i = i + 1
      if (i % numberOfShards == 0)
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

//Each replicators role ("shard-A", "shard-B") corresponds to one ShardEntity
class ReplicatorEntity(replicator: ActorRef) extends Actor with ActorLogging {
  override def preStart = {
    log.info("Allocate ReplicatorEntity")
  }

  //track local history
  var ids = Set.empty[Int]

  override def receive = {
    case cmd: Command =>
      ids += cmd.i
      log.info(ids.mkString(","))
      replicator forward cmd
  }
}