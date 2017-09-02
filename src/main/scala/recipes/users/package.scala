package recipes.users

import akka.cluster.ddata.{Key, ORMap, ReplicatedData}
import recipes.users.crdt.{CrdtUsers, VersionedUsers}

case class User(id: Long, login: String, active: Boolean)

//object UsersKey extends Key[ORMap[String, VersionedUsers]]("users-by-shard")

//One bucket may contain several
case class UsersBucket(bucketNumber: Int) extends Key[ORMap[String, CrdtUsers]](bucketNumber.toString)

case class VersionedUsersBucket(bucketNumber: Int) extends Key[VersionedUsers](bucketNumber.toString)

trait Partitioner[+T <: ReplicatedData] {
  type ReplicatedKey <: Key[T]
  //30 entities and 6 buckets.
  //Each bucket contains 5 entities which means instead of one ORMap at least (30/5) = 6 ORMaps will be used
  protected val maxNumber = 30l
  protected val buckets = Array(5l, 10l, 15l, 20l, 25, maxNumber)

  def getKey(key: Long): ReplicatedKey
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
  override def getKey(key: Long) = {
    import scala.collection.Searching._
    val index = math.abs(key % maxNumber)
    UsersBucket(buckets.search(index).insertionPoint)
  }
}


trait VersionVectorPartitioner extends Partitioner[VersionedUsers] {
  override type ReplicatedKey = VersionedUsersBucket
  override def getKey(key: Long) = {
    import scala.collection.Searching._
    val index = math.abs(key % maxNumber)
    VersionedUsersBucket(buckets.search(index).insertionPoint)
  }
}