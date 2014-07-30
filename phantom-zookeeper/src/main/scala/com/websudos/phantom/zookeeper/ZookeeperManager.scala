/*
 *
 *  * Copyright 2014 newzly ltd.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.websudos.phantom.zookeeper

import java.net.InetSocketAddress

import scala.collection.JavaConverters._

import org.slf4j.{Logger, LoggerFactory}

import com.datastax.driver.core.Cluster
import com.twitter.conversions.time._
import com.twitter.finagle.exp.zookeeper.ZooKeeper
import com.twitter.finagle.exp.zookeeper.client.ZkClient
import com.twitter.util.{Await, Timer, Try}

trait ZookeeperManager {

  protected[this] val store: ClusterStore

  def cluster: Cluster = store.cluster

  val logger: Logger

  /**
   * Boolean that keeps track of the connection status of the ZooKeeper rich client.
   * The client doesn't maintain status for itself and doesn't connect automatically before retrieving data.
   */
  private[this] var connectionStatus = false

  /**
   * Allows extending classes to connect to the ZooKeeper server using the RichClient interface provided in this trait.
   * The check is synchronized to prevent concurrent connection attempts which result in fatal errors.
   */
  protected[this] def connectIfNotConnected() = synchronized {
    if (!connectionStatus) {
      logger.info("Connecting to Zookeeper instance")
      Await.ready(store.zkClient.connect(), 2.seconds)
      connectionStatus = true
    } else {
      logger.info("Already connected to Zookeeper instance")
    }
  }

}

class EmptyClusterStoreException extends RuntimeException("Attempting to retrieve Cassandra cluster reference before initialisation")


trait ClusterStore {
  protected[this] var clusterStore: Cluster = null
  protected[this] var zkClientStore: ZkClient = null

  var inited = false

  def store(cluster: Cluster): Unit = synchronized {
    if (!inited) {
      clusterStore = cluster
      inited = true
    }
  }

  def storeClient(client: ZkClient): Unit = synchronized {
    if (!inited) {
      zkClientStore = client
      inited = true
    }
  }

  def cluster: Cluster = {
    if (inited) {
      clusterStore
    } else {
      throw new EmptyClusterStoreException
    }
  }

  def zkClient: ZkClient = {
    if (inited) {
      zkClientStore
    } else {
      throw new EmptyClusterStoreException
    }
  }
}

object DefaultClusterStore extends ClusterStore

class DefaultZookeeperManager extends ZookeeperManager {

  lazy val logger = LoggerFactory.getLogger("com.websudos.phantom.zookeeper")

  val store = DefaultClusterStore


  def initIfNotInited(connector: ZookeeperConnector) = synchronized {
    if (!store.inited) {
      store.storeClient(ZooKeeper.newRichClient(connector.connectorString))
      connectIfNotConnected()

      Timer.Nil.doLater(2.seconds) {
        store.store(Cluster.builder()
          .addContactPointsWithPorts(hostnamePortPairs.asJava)
          .withoutJMXReporting()
          .withoutMetrics()
          .build())
      }
    }
  }

  def hostnamePortPairs: Seq[InetSocketAddress] = {
    if (store.inited) {
      val mapped = store.zkClient.getData("/cassandra", watch = false) map {
        res => Try {
          val data = new String(res.data)
          data.split("\\s*,\\s*").map(_.split(":")).map {
            case Array(hostname, port) => new InetSocketAddress(hostname, port.toInt)
          }.toSeq
        } getOrElse Seq.empty[InetSocketAddress]
      }

      val res = Await.result(mapped, 3.seconds)
      logger.info("Extracting Cassandra ports from ZooKeeper")
      logger.info(s"Parsing from ${res.mkString(" ")}")
      res
    } else {
      throw new EmptyClusterStoreException()
    }

  }
}

object DefaultZookeeperManagers {
  val manager = new DefaultZookeeperManager
}
