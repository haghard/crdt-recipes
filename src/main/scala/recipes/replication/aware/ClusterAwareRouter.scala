package recipes.replication.aware

import akka.actor.{Actor, ActorLogging, ActorPath, Address, Props}
import akka.cluster.{Cluster, Member, MemberStatus}
import akka.cluster.ClusterEvent._
import recipes.hashing
import recipes.replication.routing.RouterWriter.Tick0

import scala.collection.immutable.SortedSet
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success}

object ClusterAwareRouter {

  case class Replica(a: Address) extends Comparable[Replica] {
    override def compareTo(other: Replica): Int =
      Address.addressOrdering.compare(a, other.a)
  }

  case object WriteAck

  val StoragePath = "/user/storage"

  //def toShard(a: akka.actor.Address) = s"${a.host.get}:${a.port.get}"

  def props(cluster: Cluster, interval: FiniteDuration, startWith: Int, rf: Int) =
    Props(new ClusterAwareRouter(cluster, interval, startWith, rf))
}

class ClusterAwareRouter(cluster: Cluster, interval: FiniteDuration, startWith: Int, RF: Int) extends Actor
  with ActorLogging with akka.actor.Timers {

  import ClusterAwareRouter._
  import akka.pattern.ask
  import scala.concurrent.duration._

  val writeTO = akka.util.Timeout(1.second)
  implicit val _ = context.dispatcher

  timers.startPeriodicTimer(Tick0, Tick0, interval)

  val hash = hashing.Rendezvous[Replica]

  override def postStop(): Unit =
    cluster.unsubscribe(self)

  override def preStart = {
    cluster.subscribe(self, classOf[ClusterDomainEvent])
  }

  def run(clusterMembers: SortedSet[Member], i: Int): Receive = {
    case MemberUp(member) =>
      log.info("MemberUp = {}", member.address)
      hash.add(Replica(member.address))
      context become run(clusterMembers + member, i)

    case MemberExited(member) =>
      log.info("MemberExited = {}", member.address)

    case ReachableMember(member) =>
      log.info("ReachableMember = {}", member.address)

    case UnreachableMember(member) =>
      log.info("UnreachableMember = {}", member.address)

    case MemberRemoved(member, prev) =>
      if (prev == MemberStatus.Exiting)
        log.info("{} gracefully exited", member.address)
      else
        log.info("{} downed after being Unreachable", member.address)

      hash.remove(Replica(member.address))
      context become run(clusterMembers - member, i)

    case state: CurrentClusterState =>
      log.info("CurrentClusterState state = {}", state.members)
      state.members.foreach(m => hash.add(Replica(m.address)))
      context become run(state.members, i)

    case Tick0 =>
      //aka preference list
      val replicas: Set[Replica] = hash.shardFor(i.toString, RF)

      log.info("{} goes to [{}]", i, replicas.map { _.a }.mkString(",") )

      val selections = replicas
        .map { r =>
          context.actorSelection(ActorPath.fromString(r.a.toString + StoragePath))
        }

      val writes = selections.map(s => ((s ask i)(writeTO)).mapTo[WriteAck.type])
      Future.sequence(writes).onComplete {
        case Success(r) =>
        case Failure(ex) =>
          log.error(ex, "Write error for value {}. Replica|s is|are available", i)
          //self ! Kill
      }
      context become run(clusterMembers, i + 1)
  }

  override def receive =
    run(SortedSet[Member](), startWith)
}