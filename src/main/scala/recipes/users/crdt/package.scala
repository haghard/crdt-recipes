package recipes.users.crdt

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

case class SortedEntry(version: Long = 0l, users: TreeSet[User] = new TreeSet[User]()(
  (x: User, y: User) => if (x.id < y.id) -1 else if (x.id > y.id) 1 else 0)) extends CrdtEntryLike[User] {

  override def +(u: User, version: Long) = copy(version = version, users = users + u)

  override def -(u: User, version: Long) = copy(version = version, users = users - u)

  override def replace(oldElement: User, newElement: User, version: Long) =
    copy(version = version, users = users - oldElement + newElement)

  override def toString = users.map(_.id).mkString(",")
}

case class VersionedUsers(underlying: CrdtEntryLike[User] = SortedEntry()) extends ReplicatedData {
  type T = VersionedUsers

  def add(user: User, version: Long): VersionedUsers =
    copy(underlying = underlying + (user, version))

  def remove(element: User, version: Long): VersionedUsers =
    copy(underlying = underlying - (element, version))

  def update(d: User, u: User, version: Long): VersionedUsers =
    copy(underlying = underlying.replace(d, u, version))

  def elements: Iterable[User] = underlying.users

  override def merge(that: VersionedUsers): VersionedUsers = {
    //for debug
    if (underlying.version != that.underlying.version) {
      println(s"concurrent:[${underlying.version} vs ${that.underlying.version}]")
    } else {
      println(s"[${underlying.version} vs ${that.underlying.version}]")
    }
    if (underlying.version > that.underlying.version) this else that
  }
}