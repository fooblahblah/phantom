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
import scala.concurrent._

import org.slf4j.{Logger, LoggerFactory}

import com.datastax.driver.core.{Session, Cluster}
import com.twitter.conversions.time._
import com.twitter.finagle.exp.zookeeper.ZooKeeper
import com.twitter.finagle.exp.zookeeper.client.ZkClient
import com.twitter.util.{Await, Timer, Try}

trait ZookeeperManager {

  protected[this] val store: ClusterStore

  def cluster: Cluster = store.cluster

  def session: Session = store.session

  val logger: Logger
}

class EmptyClusterStoreException extends RuntimeException("Attempting to retrieve Cassandra cluster reference before initialisation")


trait ClusterStore {
  protected[this] var clusterStore: Cluster = null
  protected[this] var zkClientStore: ZkClient = null
  protected[this] var _session: Session = null

  var inited = false

  lazy val logger = LoggerFactory.getLogger("com.websudos.phantom.zookeeper")

  def hostnamePortPairs: Seq[InetSocketAddress] = {
    if (inited) {
      val mapped = zkClientStore.getData("/cassandra", watch = false) map {
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

  def initStore(connector: ZookeeperConnector): Unit = synchronized {
    if (!inited) {
      zkClientStore = ZooKeeper.newRichClient(connector.connectorString)
      Await.ready(zkClientStore.connect(), 2.seconds)

      Timer.Nil.doLater(1.seconds) {
        clusterStore = Cluster.builder()
          .addContactPointsWithPorts(hostnamePortPairs.asJava)
          .withoutJMXReporting()
          .withoutMetrics()
          .build()


        _session = blocking {
          val s = cluster.connect()
          s.execute(s"CREATE KEYSPACE IF NOT EXISTS ${connector.keySpace} WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};")
          s.execute(s"use ${connector.keySpace};")
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

  lazy val logger = LoggerFactory.getLogger("com.websudos.phantom.zookeeper")

  val store = DefaultClusterStore

  /**
   * This will initialise the Cassandra cluster connection based on the ZooKeeper connector settings.
   * It will connector to ZooKeeper, fetch the Cassandra sequence of HOST:IP pairs, and create a cluster + session for the mix.
   * @param connector The ZooKeeper connector instance, where the ZooKeeper address and Cassandra keySpace is specified.
   */
  def initIfNotInited(connector: ZookeeperConnector) = store.initStore(connector)
}

object DefaultZookeeperManagers {
  val manager = new DefaultZookeeperManager
}
