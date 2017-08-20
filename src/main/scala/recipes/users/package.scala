package recipes.users

import akka.cluster.ddata.{Key, ORMap}
import recipes.users.crdt.VersionedUsers

case class User(id: Long, login: String, active: Boolean)

object UsersKey extends Key[ORMap[String, VersionedUsers]]("users-by-shard")

/*
case class TopLevelUsersKey(hash: String) extends Key[ORMap[String, VersionedUsers]](hash)

trait Partitioner {
  def getKey(key: String): TopLevelUsersKey
}
*/
