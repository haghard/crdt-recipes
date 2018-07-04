package recipes.shardedReplica.sharding

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.cluster.sharding.{ClusterSharding, ClusterShardingSettings, ShardRegion}
import recipes.shardedReplica.Replicator2
import recipes.shardedReplica.sharding.ShardWriter.Command

import scala.concurrent.duration.FiniteDuration

object ShardWriter {

  object Tick

  case class Command(i: Int)

  def props(system: ActorSystem, hostedShard: String, proxyShard: List[String],
    interval: FiniteDuration, startWith: Int) =
    Props(new ShardWriter(system, hostedShard, proxyShard, interval, startWith))
}

class ShardWriter(system: ActorSystem, hostedShards: String, proxyShard: List[String],
  interval: FiniteDuration, startWith: Int) extends Actor with ActorLogging {

  import ShardWriter._
  import system.dispatcher

  system.scheduler.schedule(interval, interval, self, Tick)

  //val shardNames = List(hostedShards, proxyShard)


  /*def entityId: ShardRegion.ExtractEntityId = {
    case msg @ Command(id) => ((id % shardNames.size).toString, msg)
  }

  def shardId: ShardRegion.ExtractShardId = {
    case Command(id) => shardNames(id % shardNames.size)
    case ShardRegion.StartEntity(id) => shardNames(id.hashCode % shardNames.size)
  }*/


  def shard(role: String) = {
    def entityId: ShardRegion.ExtractEntityId = {
      case msg @ Command(_) => (role.toString, msg)
    }

    def shardId: ShardRegion.ExtractShardId = {
      case Command(_) => role
      case ShardRegion.StartEntity(_) => role
    }

    val replicator = system.actorOf(Replicator2.props(system, role), s"replicator-$role")
    log.info(s"Create local shard for {}", role)
    (role, ClusterSharding(system).start(
      typeName = "domain",
      entityProps = DomainEntity.props(replicator),
      settings = ClusterShardingSettings(system).withRole(role).withRememberEntities(true),
      extractEntityId = entityId,
      extractShardId = shardId
    ))
  }

  //it will delegate messages to other `ShardRegion` actors on other nodes, but not host any entity actors itself.
  def proxy(role: String) = {
    def entityId: ShardRegion.ExtractEntityId = {
      case msg @ Command(_) => (role, msg)
    }

    def shardId: ShardRegion.ExtractShardId = {
      case Command(_) => role
      case ShardRegion.StartEntity(_) => role
    }

    log.info(s"Create proxy for {}", role)
    (role + "-proxy", ClusterSharding(system).startProxy(
      typeName = "domain",
      role = Some(role),
      extractEntityId = entityId,
      extractShardId = shardId
    ))
  }

  val shardsWithRoles = shard(hostedShards) :: proxyShard.map(proxy(_))
    //List(shard(hostedShards), proxy(proxyShard))

  val shardNames = shardsWithRoles.map(_._1)
  val shardRegions = shardsWithRoles.map(_._2)

  val shards = shardRegions.zipWithIndex./:(Map.empty[Int, ActorRef]) { (acc, c) =>
    acc + (c._2 -> c._1)
  }

  var i = startWith

  override def receive = active(0)

  def active(index: Int): Receive = {
    case Tick =>
      val ind = index % shardNames.size
       val role = shardNames(ind)
       val shardRegion = shardRegions(ind)
       log.info("writer pick {} for message {}", role, i)
       shardRegion ! Command(i)
       i = i + 1
       if(i % shardNames.size == 0)
         context.become(active(index + 1))
  }
}


object DomainEntity {
  def props(replicator: ActorRef) = Props(new DomainEntity(replicator))
}

class DomainEntity(replicator: ActorRef) extends Actor with ActorLogging {
  override def receive = {
    case cmd: Command =>
      replicator forward cmd
  }
}
