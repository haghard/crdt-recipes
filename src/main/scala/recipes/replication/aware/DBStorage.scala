package recipes.replication.aware

import java.util.UUID

import akka.actor.{Actor, ActorLogging, Props}
import recipes.replication.aware.ClusterAwareRendezvousRouter.SuccessfulWrite

object DBStorage {
  def props = Props(new DBStorage)
}

class DBStorage extends Actor with ActorLogging {

  override def preStart(): Unit = {
    //org.rocksdb.RocksDB.loadLibrary()
  }

  var count = 0
  
  override def postStop(): Unit =
    log.info("CNT: {}",  count)

  override def receive: Receive = {
    case uuid: UUID =>
      count += 1
      sender() ! SuccessfulWrite(uuid)
  }
}