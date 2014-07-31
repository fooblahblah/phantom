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

import scala.concurrent.blocking
import scala.collection.JavaConverters._

import org.slf4j.{Logger, LoggerFactory}

import com.datastax.driver.core.{Session, Cluster}
import com.twitter.conversions.time._
import com.twitter.finagle.exp.zookeeper.ZooKeeper
import com.twitter.finagle.exp.zookeeper.client.ZkClient
import com.twitter.util.{Await, Future, Timer, Try}

trait ZookeeperManager {

  protected[this] val store: ClusterStore

  def cluster: Cluster = store.cluster

  def session: Session = store.session

  val logger: Logger

  protected[zookeeper] val envString = "TEST_ZOOKEEPER_CONNECTOR"

  protected[this] val defaultAddress = new InetSocketAddress("localhost", 2181)
}

class EmptyClusterStoreException extends RuntimeException("Attempting to retrieve Cassandra cluster reference before initialisation")


trait ClusterStore {
  protected[this] var clusterStore: Cluster = null
  protected[this] var zkClientStore: ZkClient = null
  protected[this] var _session: Session = null

  var inited = false

  lazy val logger = LoggerFactory.getLogger("com.websudos.phantom.zookeeper")

  def hostnamePortPairs: Future[Seq[InetSocketAddress]] = {
    if (inited) {
      zkClientStore.getData("/cassandra", watch = false) map {
        res => Try {
          val data = new String(res.data)
          data.split("\\s*,\\s*").map(_.split(":")).map {
            case Array(hostname, port) => new InetSocketAddress(hostname, port.toInt)
          }.toSeq
        } getOrElse Seq.empty[InetSocketAddress]
      }
    } else {
      Future.exception(new EmptyClusterStoreException())
    }
  }

  def initStore(keySpace: String, address: InetSocketAddress ): Unit = synchronized {
    assert(address != null)

    if (!inited) {
      val conn = s"${address.getHostName}:${address.getPort}"
      zkClientStore = ZooKeeper.newRichClient(conn)

      Console.println(s"Connecting to ZooKeeper server instance on ${conn}")

      val res = Await.result(zkClientStore.connect(), 2.seconds)

      val ports = Await.result(hostnamePortPairs, 2.seconds)

      Timer.Nil.doLater(1.seconds) {
        clusterStore = Cluster.builder()
          .addContactPointsWithPorts(ports.asJava)
          .withoutJMXReporting()
          .withoutMetrics()
          .build()


        _session = blocking {
          val s = cluster.connect()
          s.execute(s"CREATE KEYSPACE IF NOT EXISTS $keySpace WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};")
          s.execute(s"use $keySpace;")
          s
        }

        inited = true
      }
    }
  }

  def cluster: Cluster = {
    if (inited) {
      clusterStore
    } else {
      throw new EmptyClusterStoreException
    }
  }

  def session: Session = _session

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

  def defaultZkAddress: InetSocketAddress = if (System.getProperty(envString) != null) {
    val inetPair: String = System.getProperty(envString)
    val split = inetPair.split(":")

    Try {
      logger.info(s"Using ZooKeeper settings from the $envString environment variable")
      logger.info(s"Connecting to ZooKeeper address: ${split(0)}:${split(1)}")
      new InetSocketAddress(split(0), split(1).toInt)
    } getOrElse {
      logger.warn(s"Failed to parse address from $envString environment variable with value: $inetPair")
      defaultAddress
    }
  } else {
    logger.info(s"No custom settings for Zookeeper found in $envString. Using localhost:2181 as default.")
    defaultAddress
  }

  lazy val logger = LoggerFactory.getLogger("com.websudos.phantom.zookeeper")

  val store = DefaultClusterStore

  /**
   * This will initialise the Cassandra cluster connection based on the ZooKeeper connector settings.
   * It will connector to ZooKeeper, fetch the Cassandra sequence of HOST:IP pairs, and create a cluster + session for the mix.
   * @param connector The ZooKeeper connector instance, where the ZooKeeper address and Cassandra keySpace is specified.
   */
  def initIfNotInited(connector: ZookeeperConnector, address: InetSocketAddress = defaultZkAddress) = store.initStore(connector.keySpace, address)
}

object DefaultZookeeperManagers {
  val manager = new DefaultZookeeperManager
}
