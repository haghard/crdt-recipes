package recipes.replication.routing

import akka.routing.ConsistentHashingGroup
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.cluster.routing.{ClusterRouterGroup, ClusterRouterGroupSettings}
import akka.routing.ConsistentHashingRouter.ConsistentHashableEnvelope
import recipes.replication.sharding.ShardWriter.Command

import scala.concurrent.duration.FiniteDuration

object RouterWriter {

  case object Tick0

  def routeeName(shard: String) = s"routee-$shard"

  def props(system: ActorSystem, localShards: Vector[String], proxy: String,
    interval: FiniteDuration, startWith: Int) =
      Props(new RouterWriter(system, localShards, proxy, interval, startWith))
}

/*
Notes from Akka Documentation about cluster aware routes

When a node becomes unreachable or leaves the cluster the routees of that node are automatically unregistered from the router.
When new nodes join the cluster, additional routees are added to the router, according to the configuration.
Routees are also added when a node becomes reachable again, after having been unreachable.

https://doc.akka.io/docs/akka/current/cluster-usage.html#cluster-aware-routers
*/
class RouterWriter(system: ActorSystem, shards: Vector[String], proxy: String,
  interval: FiniteDuration, startWith: Int) extends Actor with ActorLogging with akka.actor.Timers {
  import RouterWriter._

  var routers: Vector[ActorRef] =
    shards.map(createRouter(_)) :+ createProxy(proxy)

  private val numberOfShards = routers.size

  timers.startPeriodicTimer(Tick0, Tick0, interval)

  //
  def createRouter(shard: String) = {
    //supervision ???
    val rName = routeeName(shard)
    system.actorOf(ReplicatorForShardR.props(system, shard), rName)

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
          allowLocalRoutees = false, //important
          useRoles = shard)
      ).props(), s"router-$shard")

    remoteRouter
  }


  override def receive =
    active(0)

  var i = startWith

  def active(index: Int): Receive = {
    case Tick0 =>
      val ind = index % numberOfShards
      val router = routers(ind)
      log.info("Picks shard {} for message {}", ind, i)
      router ! ConsistentHashableEnvelope(Command(i), i)
      i = i + 1
      context.become(active(index + 1))
  }
}