package utils

import com.typesafe.scalalogging.Logger
import org.apache.zookeeper.server.{ NIOServerCnxnFactory, ZooKeeperServer, ServerCnxnFactory }
import java.net.InetSocketAddress
import java.io.{IOException, File}


private[utils] class Zookeeper(port: Int, tempDirs: TemporaryDirectories) {
  private val LOGGER = Logger[Zookeeper]
  private var serverConnectionFactory: Option[ServerCnxnFactory] = None
  
   /**
   * Start the instance if not started.
   */
  def start() {
    LOGGER.info(s"starting Zookeeper on $port")

    try {
      val zkMaxConnections = 32
      val zkTickTime = 2000
      val zkServer = new ZooKeeperServer(tempDirs.zkSnapshotDir, tempDirs.zkLogDir, zkTickTime)
      serverConnectionFactory = Some(new NIOServerCnxnFactory())
      serverConnectionFactory.get.configure(new InetSocketAddress("localhost", port), zkMaxConnections)
      serverConnectionFactory.get.startup(zkServer)
    }
    catch {
      case e: InterruptedException => {
        Thread.currentThread.interrupt()
      }
      case e: IOException => {
        throw new RuntimeException("Unable to start ZooKeeper", e)
      }
    }
  }
  
  /**
   * Stop the instance if running.
   */
  def stop() {
    LOGGER.info(s"shutting down Zookeeper on $port")
    serverConnectionFactory match {
      case Some(f) => {
        f.shutdown
        serverConnectionFactory = None
      }
      case None =>
    }
  }
}