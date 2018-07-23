package recipes.replication.aware

import akka.actor.{Actor, ActorLogging, Props}
import org.rocksdb.RocksDB
import recipes.replication.aware.ClusterAwareRouter.WriteAck

object DBStorage {
  def props = Props(new DBStorage)
}

class DBStorage extends Actor with ActorLogging {

  override def preStart(): Unit =
    RocksDB.loadLibrary()

  override def receive: Receive = {
    case i: Int =>
      //log.info("accept: {}",  i)
      sender() ! WriteAck
  }
}