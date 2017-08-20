package recipes.users

import akka.cluster.ddata.{Key, ORMap}
import recipes.users.crdt.VersionedUsers

case class User(id: Long, login: String, active: Boolean)

//object UsersKey extends Key[ORMap[String, VersionedUsers]]("users-by-shard")

case class TopLevelUsersKey(hash: String) extends Key[ORMap[String, VersionedUsers]](hash)


trait Partitioner {
  def getKey(key: Long): TopLevelUsersKey
}

/**
 *
 * We want to split a global ORMap up in (30/5) 6 top level ORMaps.
 * Top level entries are replicated individually, which has the trade-off that different entries may not be replicated at the same time
 * and you may see inconsistencies between related entries.
 * Separated top level entries cannot be updated atomically together.
 */
trait MultiMapsPartitioner extends Partitioner {
  //30 entities and 6 buckets.
  //Each bucket contains 5 entities which means instead of one ORMap at least (30/5) = 6 ORMaps will be used
  val maxNumber = 30l
  val buckets = Array(5l, 10l, 15l, 20l, 25, maxNumber)

  override def getKey(key: Long): TopLevelUsersKey = {
    import scala.collection.Searching._
    val index = math.abs(key % maxNumber)
    TopLevelUsersKey(s"bucket:${buckets.search(index).insertionPoint}")
  }
}