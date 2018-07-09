package recipes.replication.sharding

import java.util.UUID
import recipes.replication.sharding.ShardWriter.Command
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.cluster.sharding.{ClusterSharding, ClusterShardingSettings, ShardRegion}

import scala.concurrent.duration.FiniteDuration

object ShardWriter {
  case object Tick
  case class Command(i: Int)

  def props(system: ActorSystem, hostedShard: String,
    proxy: String, interval: FiniteDuration, startWith: Int) =
    Props(new ShardWriter(system, hostedShard, proxy, interval, startWith))
}

class ShardWriter(system: ActorSystem, hostedShard: String, proxy: String, interval: FiniteDuration,
  startWith: Int) extends Actor with ActorLogging with akka.actor.Timers {
  import ShardWriter._

  timers.startPeriodicTimer(Tick, Tick, interval)

  def entityId: ShardRegion.ExtractEntityId = {
    case msg @ Command(id) => ((id % shardNames.size).toString, msg)
  }

  def shardId: ShardRegion.ExtractShardId = {
    case Command(id) => shardNames(id % shardNames.size)
    case ShardRegion.StartEntity(id) => shardNames(id.hashCode % shardNames.size)
  }

  def createShard(role: String) = {
    log.info(s"Create local shard for {}", role)
    (role, ClusterSharding(system).start(
      typeName = "replicas",
      entityProps = DomainEntity.props(role),
      settings = ClusterShardingSettings(system).withRole(role).withRememberEntities(true),
      extractEntityId = entityId,
      extractShardId = shardId
    ))
  }

  //it will delegate messages to other `ShardRegion` actors on other nodes, but not host any entity actors itself.
  def createProxy(role: String) = {
    log.info(s"Create proxy for {}", role)
    (role, ClusterSharding(system).startProxy(
      typeName = "replicas",
      role = Some(role),
      extractEntityId = entityId,
      extractShardId = shardId
    ))
  }

  val shardsWithRoles = Vector(createShard(hostedShard), createProxy(proxy))
  val shardNames = shardsWithRoles.map("shard-" + _._1)
  val shardRegions = shardsWithRoles.map(_._2)

  val shards = shardRegions.zipWithIndex./:(Map.empty[Int, ActorRef]) { (acc, c) =>
    acc + (c._2 -> c._1)
  }

  var i = startWith

  override def receive =
    active(0)

  def active(index: Int): Receive = {
    case Tick =>
      val ind = index % shardNames.size
       val role = shardNames(ind)
       val shardRegion = shardRegions(ind)
       log.info("writer pick {} for message {}", role, i)
       shardRegion ! Command(i)
       i = i + 1
       context become active(index + 1)
  }
}


object DomainEntity {
  def props(role: String) = Props(new DomainEntity(role))
}

class DomainEntity(role: String) extends Actor with ActorLogging {
  val postfix = UUID.randomUUID().toString.take(6)
  val replicator =
    context.actorOf(ReplicatorForShardS.props(role), s"entity-$role-$postfix")

  override def receive = {
    case cmd: Command =>
      replicator forward cmd
  }
}