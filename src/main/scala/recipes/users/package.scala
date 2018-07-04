package recipes.users

import akka.cluster.ddata.{Key, ORMap, ReplicatedData, VersionVector}
import recipes.users.crdt.{CrdtUsers, VersionedUsers}

case class Node(host: String, port: Int)
case class User(id: String, login: String, createdAt: Node, active: Boolean = false, online: Boolean = false)

//object UsersKey extends Key[ORMap[String, VersionedUsers]]("users-by-shard")
object UsersKey extends Key[VersionedUsers]("users")

//One bucket contains one ORMap
case class UsersBucket(bucketNumber: Int) extends Key[ORMap[String, CrdtUsers]](bucketNumber.toString)

case class UserSegment(tenant: String) extends Key[VersionedUsers](tenant)

trait Partitioner[+T <: ReplicatedData] {
  type ReplicatedKey <: Key[T]

  //akka.cluster.ddata.VersionVector.empty
  //akka.cluster.VectorClock

  //30 entities and 6 buckets.
  //Each bucket contains 5 entities which means instead of one ORMap at least (30/5) = 6 ORMaps will be used
  protected val maxNumber = 30l
  protected val buckets = Array(5l, 10l, 15l, 20l, 25, maxNumber)

  def getBucket(key: Long): ReplicatedKey
}

/**
 *
 * We want to split a global ORMap up in (30/5) = 6 top level ORMaps.
 * Top level entries are replicated individually, which has the trade-off that different entries may not be replicated at the same time
 * and you may see inconsistencies between related entries.
 * Separated top level entries cannot be updated atomically together.
 */
trait MultiMapsPartitioner extends Partitioner[ORMap[String, CrdtUsers]] {
  override type ReplicatedKey = UsersBucket
  override def getBucket(key: Long) = {
    import scala.collection.Searching._
    val index = math.abs(key % maxNumber)
    UsersBucket(buckets.search(index).insertionPoint)
  }
}


trait VersionVectorPartitioner extends Partitioner[VersionedUsers] {
  override type ReplicatedKey = UsersKey.type /*VersionedUsersBucket*/
  override def getBucket(key: Long) = {
    UsersKey
  }
}

case class CreateUser(userId: String, login: String, isActive: Boolean = false)
case class ActivateUser(userId: String, isActive: Boolean = true)
case class LoginUser(userId: String, isLogin: Boolean = true)