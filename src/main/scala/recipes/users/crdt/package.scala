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
  
object Implicits {
  implicit val ord = new cats.Order[UniqueAddress] {
    override def compare(x: UniqueAddress, y: UniqueAddress) = (x compare y)
  }
}
