package recipes.shardedReplica.routing

import akka.routing.ConsistentHashingGroup
import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.cluster.routing.{ClusterRouterGroup, ClusterRouterGroupSettings}
import akka.routing.ConsistentHashingRouter.ConsistentHashableEnvelope
import recipes.shardedReplica.ReplicatorForShard
import recipes.shardedReplica.sharding.ShardWriter.{Command, Tick}

import scala.concurrent.duration.FiniteDuration

object RouterWriter {

  def routeeName(shard: String) = s"routee-$shard"

  def props(system: ActorSystem,
    localShards: Vector[String], proxy: String, interval: FiniteDuration, startWith: Int) =
    Props(new RouterWriter(system, localShards, proxy, interval, startWith))
}

class RouterWriter(system: ActorSystem, shards: Vector[String],
  proxy: String, interval: FiniteDuration, startWith: Int) extends Actor with ActorLogging {
  import RouterWriter._

  private var i = startWith
  private val numberOfShards = shards.size

  import system.dispatcher
  system.scheduler.schedule(interval, interval, self, Tick)

  def createRouter(shard: String) = {
    //supervision ???
    val rName = routeeName(shard)
    //val replicator = system.actorOf(ReplicatorForShard.props(system, shard), rName /*shard*/)
    system.actorOf(ReplicatorForShard.props(system, shard), rName)
    //context.system.actorOf(RouteeReplicator.props(replicator), rName)

    log.info(s"*******create router for {}", shard)
    val shardRouter = system.actorOf(
      ClusterRouterGroup(
        ConsistentHashingGroup(Nil),
        ClusterRouterGroupSettings(
          totalInstances = shards.size,
          routeesPaths = List(s"/user/$rName"),
          allowLocalRoutees = true,
          useRoles = shard)
      ).props(), s"router-$shard")

    shardRouter
  }

  def createProxy(shard: String) = {
    val rName = routeeName(shard)
    log.info(s"*******create proxy-router for {}", shard)
    val remoteRouter = system.actorOf(
      ClusterRouterGroup(
        ConsistentHashingGroup(Nil),
        ClusterRouterGroupSettings(
          totalInstances = shards.size,
          routeesPaths = List(s"/user/$rName"),
          allowLocalRoutees = false,  //important
          useRoles = shard)
      ).props(), s"router-$shard")

    remoteRouter
  }

  val routers = shards.map(createRouter(_)) :+ createProxy(proxy)

  override def receive = active(0)

  def active(index: Int): Receive = {
    case Tick =>
      val ind = index % numberOfShards
      val shard = shards(ind)
      val router = routers(ind)
      log.info("writer pick shard {} for message {}", shard, i)
      router ! ConsistentHashableEnvelope(Command(i), i)
      i = i + 1
      if (i % numberOfShards == 0)
        context.become(active(index + 1))
  }
}

/*
object RouteeReplicator {
  def props(replicator: ActorRef) = Props(new RouteeReplicator(replicator))
}

class RouteeReplicator(replicator: ActorRef) extends Actor with ActorLogging {
  override def preStart =
    log.info("Allocate RouteeReplicator")

  //track local history
  var ids = Set.empty[Int]

  override def receive = {
    case cmd:Command =>
      ids += cmd.i
      log.info(ids.mkString(","))
      replicator forward cmd
  }
}
*/
