package recipes.replication.aware

import java.util.UUID

import akka.actor.{Actor, ActorLogging, Address, Props}
import akka.cluster.{Cluster, Member, MemberStatus}
import akka.cluster.ClusterEvent._
import recipes.hashing
import recipes.replication.routing.RouterWriter.Tick0

import scala.collection.immutable.SortedSet
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success}
import ClusterAwareRendezvousRouter._
import akka.pattern.ask
import scala.concurrent.duration._

object ClusterAwareRendezvousRouter {

  case class Replica(a: Address) extends Comparable[Replica] {
    override def compareTo(other: Replica): Int =
      Address.addressOrdering.compare(a, other.a)
  }

  sealed trait WriteResponse

  case class SuccessfulWrite(id: UUID) extends WriteResponse

  case class FailedWrite(id: UUID) extends WriteResponse

  val StoragePath = "/user/storage"

  def props(cluster: Cluster, interval: FiniteDuration, startWith: Int, rf: Int) =
    Props(new ClusterAwareRendezvousRouter(cluster, interval, startWith, rf))
}

class ClusterAwareRendezvousRouter(cluster: Cluster, interval: FiniteDuration, startWith: Int, RF: Int) extends Actor
  with ActorLogging with akka.actor.Timers {

  val writeTO = akka.util.Timeout(1.second)
  implicit val _ = context.dispatcher

  timers.startPeriodicTimer(Tick0, Tick0, interval)

  //val hash = hashing.Rendezvous[Replica]

  override def postStop(): Unit =
    cluster.unsubscribe(self)

  override def preStart =
    cluster.subscribe(self, classOf[ClusterDomainEvent])

  def run(liveMembers: SortedSet[Member], hash: hashing.Rendezvous[Replica], i: Int): Receive = {
    case MemberUp(member) =>
      log.info("MemberUp = {}", member.address)
      hash.add(Replica(member.address))
      context become run(liveMembers + member, hash, i)

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
        log.info("{} downed after being \"unreachable\" ", member.address)

      hash.remove(Replica(member.address))
      context become run(liveMembers - member, hash, i)

    case state: CurrentClusterState =>
      log.info("CurrentClusterState state = {}", state.members)
      state.members.foreach(m => hash.add(Replica(m.address)))
      context become run(state.members, hash, i)

    case Tick0 =>
      val uuid = UUID.randomUUID
      val replicas: Set[Replica] = hash.shardFor(uuid.toString, RF)

      log.info("replicate {} to [{}]", uuid.toString, replicas.map {
        _.a
      }.mkString(" and "))

      /*ActorPath.fromString()*/
      val selections = replicas
        .map { r =>
          val path = new StringBuilder()
            .append(r.a.toString)
            .append(StoragePath)
            .toString
          context.actorSelection(path)
        }

      val writes = selections.map(s => ((s ask uuid) (writeTO)).mapTo[WriteResponse])

      //assumes WriteConsistency == RF
      Future.sequence(writes).onComplete {
        case Success(r) =>
        //log.info("Successful replication for value {}", i)
        case Failure(ex) =>
          log.error(ex, "Write error for value {}. Replica|s is|are available", i)
        //self ! Kill
      }
      context become run(liveMembers, hash, i + 1)
  }

  override def receive =
    run(SortedSet[Member](), hashing.Rendezvous[Replica], startWith)
}