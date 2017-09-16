package recipes.users.crdt

import akka.cluster.UniqueAddress
import akka.cluster.ddata.ReplicatedData
import recipes.users.User

import scala.collection.immutable.TreeSet

trait CrdtEntryLike[T] {
  def users: Iterable[T]

  def version: Long

  def +(element: T, version: Long): CrdtEntryLike[T]

  def -(element: T, version: Long): CrdtEntryLike[T]

  def replace(oldElement: T, newElement: T, version: Long): CrdtEntryLike[T]
}

case class CrdtUsersEntry(version: Long = 0l, users: TreeSet[User] = new TreeSet[User]()(
  (x: User, y: User) => if (x.id < y.id) -1 else if (x.id > y.id) 1 else 0)) extends CrdtEntryLike[User] {

  override def +(u: User, version: Long) = copy(version = version, users = users + u)

  override def -(u: User, version: Long) = copy(version = version, users = users - u)

  override def replace(oldElement: User, newElement: User, version: Long) =
    copy(version = version, users = users - oldElement + newElement)

  override def toString = users.map(_.id).mkString(",")
}

case class CrdtUsers(underlying: CrdtEntryLike[User] = CrdtUsersEntry()) extends ReplicatedData {
  type T = CrdtUsers

  def add(user: User, version: Long): CrdtUsers =
    copy(underlying = underlying + (user, version))

  def remove(element: User, version: Long): CrdtUsers =
    copy(underlying = underlying - (element, version))

  def update(d: User, u: User, version: Long): CrdtUsers =
    copy(underlying = underlying.replace(d, u, version))

  def elements: Iterable[User] = underlying.users

  override def merge(that: CrdtUsers): CrdtUsers = {
    //debug
    /*
    if (underlying.version != that.underlying.version) {
      println(s"concurrent:[${underlying.version} vs ${that.underlying.version}]")
    } else {
      println(s"[${underlying.version} vs ${that.underlying.version}]")
    }*/
    if (underlying.version > that.underlying.version) this else that
  }
}


trait VersionedEntryLike[T] {
  def users: Iterable[T]

  def version: VersionVector[UniqueAddress]

  def +(element: T, node: UniqueAddress): VersionedEntryLike[T]

  def -(element: T, node: UniqueAddress): VersionedEntryLike[T]

  def replace(oldElement: T, newElement: T, node: UniqueAddress): VersionedEntryLike[T]
}

case class VersionedEntry(
  users: Set[User] = new TreeSet[User]()((x: User, y: User) => if (x.id < y.id) -1 else if (x.id > y.id) 1 else 0),
  version: VersionVector[UniqueAddress] = VersionVector.empty[UniqueAddress](Implicits.ord)) extends VersionedEntryLike[User] {

  override def +(u: User, node: UniqueAddress) =
    copy(users = users + u, version = version + node)

  override def -(u: User, node: UniqueAddress) =
    copy(users = users - u, version = version + node)

  override def replace(oldElement: User, newElement: User, node: UniqueAddress) =
    copy(users = users - oldElement + newElement, version = version + node)

  override def toString = users.map(_.id).mkString(",")
}

case class VersionedUsers(
  owner: UniqueAddress,
  underlying: VersionedEntryLike[User] = VersionedEntry()) extends ReplicatedData {
  type T = VersionedUsers

  def add(user: User, node: UniqueAddress): VersionedUsers =
    copy(underlying = underlying + (user, node))

  def remove(user: User, node: UniqueAddress): VersionedUsers =
    copy(underlying = underlying - (user, node))

  def update(d: User, u: User, node: UniqueAddress): VersionedUsers =
    copy(underlying = underlying.replace(d, u, node))

  def elements: Iterable[User] = underlying.users

  override def merge(that: VersionedUsers): VersionedUsers = {
    val localValue = underlying.version.elems.get(owner)
    val remoteValue = that.underlying.version.elems.get(owner)
    //underlying.version.merge(that.underlying.version)

    if(underlying.version < that.underlying.version) {
      that
    } else if (underlying.version == that.underlying.version) {
      that
    } else if (underlying.version > that.underlying.version) {
      this
    } else if (underlying.version <> that.underlying.version) {
      println(s"!!!!!!!!!!!!!!!!: ${localValue} vs ${remoteValue}")
      this
    } else {
      println(s"Unexpected: ${localValue} vs ${remoteValue}")
      this
    }

    /*if(underlying.version <> that.underlying.version) {
      println(s"!!!!!!!!!!!!!!!!: ${localValue} vs ${remoteValue}")
      this
    } else {
      println(s"${owner.address}: Gossip value $remoteValue dominates local:${localValue}")
      that
    }*/
  }
}

import scala.language.higherKinds
import cats.Monoid
import cats.syntax.semigroup._
import cats.syntax.foldable._
 

trait BoundedSemiLattice[A] extends Monoid[A] {
  def combine(a1: A, a2: A): A
  def empty: A
}

object BoundedSemiLattice {

  implicit object intBoundedSemiLatticeInstance extends BoundedSemiLattice[Int] {
    override def combine(a1: Int, a2: Int): Int = a1 max a2
    override val empty: Int = 0
  }

  implicit def setBoundedSemiLatticeInstance[A]: BoundedSemiLattice[Set[A]] =
    new BoundedSemiLattice[Set[A]] {
      override def combine(a1: Set[A], a2: Set[A]): Set[A] = a1 union a2
      override val empty: Set[A] = Set.empty[A]
  }


  /*
    increment requires a monoid;
    total requires a commutative monoid
    merge required an idempotent commutative monoid, also called a bounded semilattice.
   */

  trait GCounter[F[_,_],K, V] {
    /*
     *
     */
    def increment(f: F[K, V])(k: K, v: V)(implicit m: Monoid[V]): F[K, V]
    /*

     */
    def total(f: F[K, V])(implicit m: Monoid[V]): V
    /*
      Requires a bounded semilattice (or idempotent commutative monoid).
      We rely on commutivity to ensure that machine A merging with machine B yields the same result
      as machine B merging with machine A.
      We need associativity to ensure we obtain the correct result when three or more machines are merging data.
      We need an identity element to initialise empty counters.
      Finally, we need an additional property, called idempotency, to ensure that if two machines hold the same data
      in a per-machine counter, merging data will not lead to an incorrect result.
    */
    def merge(f1: F[K, V], f2: F[K, V])(implicit b: BoundedSemiLattice[V]): F[K, V]
  }

  object GCounter {
    implicit def mapGCounter[K, V]: GCounter[Map, K, V] =
      new GCounter[Map, K, V] {
        import cats.instances.map._

        def increment(fa: Map[K, V])(k: K, v: V)(implicit m: Monoid[V]): Map[K, V] =
          fa + (k -> (fa.getOrElse(k, m.empty) |+| v))

        def total(f: Map[K, V])(implicit m: Monoid[V]): V =
          f.foldMap(identity)

        def merge(f1: Map[K, V], f2: Map[K, V])(implicit b: BoundedSemiLattice[V]): Map[K, V] =
          f1 |+| f2
      }

    def apply[F[_,_],K, V](implicit g: GCounter[F, K, V]) = g
  }

  val g1 = Map("a" -> 7, "b" -> 3)
  val g2 = Map("a" -> 2, "b" -> 5)

  GCounter[Map, String, Int].merge(g1, g2)
  GCounter[Map, String, Int].total(g1)
  

}
  
object Implicits {
  implicit val ord = new cats.Order[UniqueAddress] {
    override def compare(x: UniqueAddress, y: UniqueAddress) = (x compare y)
  }
}
