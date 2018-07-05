package akka.cluster.ddata

import java.io.File

import akka.actor.{Actor, ActorLogging, Props}
import akka.cluster.Cluster
import akka.cluster.ddata.DurableStore._
import akka.serialization.{SerializationExtension, SerializerWithStringManifest}
import com.typesafe.config.Config
import org.rocksdb.RocksDB

object RocksDurableStore {
  def props(config: Config): Props =
    Props(new RocksDurableStore(config))
}


//see akka.cluster.ddata.DurableStore for implementation


//https://github.com/facebook/rocksdb/blob/master/java/samples/src/main/java/RocksDBColumnFamilySample.java
//https://github.com/facebook/rocksdb/blob/master/java/samples/src/main/java/RocksDBSample.java
//org.rocksdb.RocksDB.loadLibrary()
final class RocksDurableStore(config: Config) extends Actor with ActorLogging {
  val serialization = SerializationExtension(context.system)
  val serializer = serialization.serializerFor(classOf[DurableDataEnvelope]).asInstanceOf[SerializerWithStringManifest]
  val manifest = serializer.manifest(new DurableDataEnvelope(Replicator.Internal.DeletedData))

  val dir = config.getString("rocks.dir") match {
    case path if path.endsWith("ddata") =>
      new File(s"$path-${context.system.name}-${self.path.parent.name}-${Cluster(context.system).selfAddress.port.get}")
    case path =>
      new File(path)
  }


  override def receive: Receive = {
    RocksDB.loadLibrary()
    init
  }

  /*val db: RocksDB = {
    val options: Options = new Options().setCreateIfMissing(true)
    RocksDB.open(options, dir.getPath)
  }*/

  //val dataBytes: Array[Byte] = db.get(key)

  def init: Receive = {
    case LoadAll =>
      log.info("************** LoadAll ******")
      // no files to load
      sender() ! LoadAllCompleted
      context.become(active)
  }

  def active: Receive = {
    case Store(key, data, reply) =>
      log.info("Store key: {}", key)
      reply match {
        case Some(StoreReply(successMsg, _, replyTo)) =>
          replyTo ! successMsg
        case None =>
      }
  }
}