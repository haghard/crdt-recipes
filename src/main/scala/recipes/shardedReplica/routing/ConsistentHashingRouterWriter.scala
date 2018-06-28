package recipes.shardedReplica.routing

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.cluster.routing.{ClusterRouterGroup, ClusterRouterGroupSettings}
import akka.routing.ConsistentHashingGroup
import akka.routing.ConsistentHashingRouter.ConsistentHashableEnvelope
import recipes.shardedReplica.ReplicatorForRole
import recipes.shardedReplica.sharding.ShardWriter.{Command, Tick}

import scala.concurrent.duration.FiniteDuration

object ConsistentHashingRouterWriter {
  def props(system: ActorSystem, shards: Vector[String], interval: FiniteDuration, startWith: Int) =
    Props(new ConsistentHashingRouterWriter(system, shards, interval, startWith))
}

class ConsistentHashingRouterWriter(system: ActorSystem, shards: Vector[String], interval: FiniteDuration,
  startWith: Int) extends Actor with ActorLogging {

  private var i = startWith
  private val numberOfShards = shards.size

  import system.dispatcher
  system.scheduler.schedule(interval, interval, self, Tick)

  def create(shard: String) = {
    //routee
    context.system.actorOf(RouteeReplicator.props(
      system.actorOf(ReplicatorForRole.props(system, shard), shard)
    ), name = s"routee-replicator-$shard")

    val shardRouter = system.actorOf(
      ClusterRouterGroup(
        ConsistentHashingGroup(Nil),
        ClusterRouterGroupSettings(
          totalInstances = shards.size,
          routeesPaths = List(s"/user/routee-replicator-$shard"),
          allowLocalRoutees = true,
          useRoles = shard)
      ).props(), name = s"shard-router-$shard")

    shardRouter
  }

  val routers = shards.map(create(_))

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


object RouteeReplicator {
  def props(replicator: ActorRef) = Props(new RouteeReplicator(replicator))
}

/*
[akka://counts@127.0.0.1:2550/user/routee-replicator-shard-A] 20,21,13,105,100
[akka://counts@127.0.0.1:2551/user/routee-replicator-shard-A] 101,0,5,1,9,17,12,16,104,8,4

[akka://counts@127.0.0.1:2550/user/routee-replicator-shard-B] 106,2,22,103,18,11,19,107,15
[akka://counts@127.0.0.1:2551/user/routee-replicator-shard-B] 10,14,6,102,7,3,23
 */
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

