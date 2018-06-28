package recipes.users.crdt

import akka.cluster.UniqueAddress
import akka.cluster.ddata.ReplicatedData
import javax.xml.bind.DatatypeConverter
import recipes.users.{Node, User}

import scala.collection.immutable.TreeSet

trait CrdtEntryLike[T] {
  def users: Iterable[T]

  def version: Long

  def +(element: T, version: Long): CrdtEntryLike[T]

  def -(element: T, version: Long): CrdtEntryLike[T]

  def replace(oldElement: T, newElement: T, version: Long): CrdtEntryLike[T]
}

case class CrdtUsersEntry(
  version: Long = 0l,
  users: TreeSet[User] = new TreeSet[User]()((x: User, y: User) => if (x.id < y.id) -1 else if (x.id > y.id) 1 else 0)
) extends CrdtEntryLike[User] {

  override def +(u: User, version: Long) = copy(version = version, users = users + u)

  override def -(u: User, version: Long) = copy(version = version, users = users - u)

  override def replace(oldElement: User, newElement: User, version: Long) =
    copy(version = version, users = users - oldElement + newElement)

  override def toString = users.map(_.id).mkString(",")
}

//Try to use DeltaReplicatedData instead
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

  def versions: VersionVector[Node]

  def +(element: T, node: Node): VersionedEntryLike[T]

  def -(element: T, node: Node): VersionedEntryLike[T]

  def replace(oldElement: T, newElement: T, node: Node): VersionedEntryLike[T]
}

case class VersionedEntry(
  override val users: Set[User] = new TreeSet[User]()((x: User, y: User) => if (x.id < y.id) -1 else if (x.id > y.id) 1 else 0),
  override val versions: VersionVector[Node] = VersionVector.empty[Node](Implicits.addOrd)
) extends VersionedEntryLike[User] {

  override def +(u: User, node: Node) =
    copy(users = users + u, versions = versions + node)

  override def -(u: User, node: Node) =
    copy(users = users - u, versions = versions + node)

  override def replace(oldElement: User, newElement: User, node: Node) =
    copy(users = users - oldElement + newElement, versions = versions + node)

  override def toString = users.map(_.id).mkString(",")
}

object ContentUtils {
  private val MD5 = java.security.MessageDigest.getInstance("MD5")

  def isDiff(thisUser: User, thatUser: User) = {
    MD5.update(thisUser.toString.getBytes)
    val thisHash = DatatypeConverter.printHexBinary(MD5.digest).toUpperCase
    MD5.update(thatUser.toString.getBytes)
    val thatHash = DatatypeConverter.printHexBinary(MD5.digest).toUpperCase
    thisHash != thatHash
  }
}


//make in serializable
case class VersionedUsers(owner: Node, underlying: VersionedEntryLike[User] = VersionedEntry()) extends ReplicatedData {
  type T = VersionedUsers

  def add(user: User, node: Node): VersionedUsers =
    copy(underlying = underlying + (user, node))

  def remove(user: User, node: Node): VersionedUsers =
    copy(underlying = underlying - (user, node))

  def update(user: User, updatedUser: User, node: Node): VersionedUsers =
    copy(underlying = underlying.replace(user, updatedUser, node))

  def elements: Iterable[User] = underlying.users

  override def merge(that: VersionedUsers): VersionedUsers = {
    if (underlying.versions < that.underlying.versions) {
      that
    } else if (underlying.versions == that.underlying.versions) {
      that
    } else if (underlying.versions > that.underlying.versions) {
      this
    } else if (underlying.versions <> that.underlying.versions) {
      println(s"*****Concurrent versions on ${this.owner} *****")

      //merge vvs
      val mergedVV = underlying.versions merge that.underlying.versions

      val thisUsers =
        this.underlying.users./:(Map[String, User]())((acc, c) => acc + (c.id -> c))

      val thatUsers =
        that.underlying.users./:(Map[String, User]())((acc, c) => acc + (c.id -> c))

      val allIds =
        this.underlying.users.map(_.id).toSet.union(that.underlying.users.map(_.id).toSet)

      val mergedUsers = allIds./:(Set.empty[User]) { (acc, id) =>
        val thisUser = thisUsers.get(id)
        val thatUser = thatUsers.get(id)
        val dominatedUserVersion =
          (thisUser, thatUser) match {
            case (Some(thisUser), Some(thatUser)) =>
              //if(thisUser.online != thatUser.online || thisUser.active != thatUser.active) {
              if(ContentUtils.isDiff(thisUser, thatUser)) {
                //We operate under assumption that we update a particular user on the node where this user had been created
                val createdAt = thisUser.createdAt
                val localV = underlying.versions.version(createdAt)
                val remoteV = that.underlying.versions.version(createdAt)
                if (localV > remoteV) {
                  println(s"[$localV:$thisUser] vs [$remoteV:$thatUser winner $thisUser]")
                  thisUser
                } else {
                  println(s"[$localV:$thisUser] vs [$remoteV:$thatUser winner $thatUser]")
                  thatUser
                }
              } else thisUser
            case (None, Some(thatUser)) => thatUser
            case (Some(thisUser), None) => thisUser
            case (None, None) =>
              throw new Exception(s"User $id vanished")
          }
        acc + dominatedUserVersion
      }

      val mergedEntry = VersionedEntry(mergedUsers, mergedVV)
      VersionedUsers(this.owner, mergedEntry)
    } else {
      println("Unexpected branch !!!!")
      throw new Exception("Unexpected branch !!!!")
    }
  }
}

object Implicits {
  implicit val ordCats = new cats.Order[UniqueAddress] {
    override def compare(x: UniqueAddress, y: UniqueAddress) = (x compare y)
  }

  implicit val addOrd = new scala.Ordering[Node] {
    override def compare(x: Node, y: Node) =
    //Member.addressOrdering.compare(x, y)
      Ordering.fromLessThan[Node] { (x, y) =>
        if (x.host != y.host) x.host.compareTo(y.host) < 0
        else if (x.port != y.port) x.port < y.port
        else false
      }.compare(x, y)
  }
}
