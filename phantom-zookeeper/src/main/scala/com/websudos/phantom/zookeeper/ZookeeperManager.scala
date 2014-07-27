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

import scala.collection.JavaConverters._
import scala.util.DynamicVariable

import org.slf4j.{ Logger, LoggerFactory }

import com.datastax.driver.core.Cluster

trait ZookeeperManager {

  protected[this] val store: ClusterStore

  def cluster: Cluster = store.value

  val logger: Logger
}

class EmptyClusterStoreException extends RuntimeException("Attempting to retrieve Cassandra cluster reference before initialisation")


trait ClusterStore {
  protected[this] val clusterStore = new DynamicVariable[Cluster](null)

  private[this] var inited = false

  def store(cluster: Cluster): Unit = synchronized {
    if (!inited) {
      clusterStore.value_=(cluster)
      inited = true
    }
  }

  def isInited: Boolean = inited

  def value: Cluster = {
    if (inited) {
      clusterStore.value
    } else {
      throw new EmptyClusterStoreException
    }
  }
}

object DefaultClusterStore extends ClusterStore

class DefaultZookeeperManager(connector: ZookeeperConnector) extends ZookeeperManager {

  lazy val logger = LoggerFactory.getLogger("com.websudos.phantom.zookeeper")

  val store = DefaultClusterStore

  if (!store.isInited) {
    store.store(Cluster.builder()
      .addContactPointsWithPorts(connector.hostnamePortPairs.asJava)
      .withoutJMXReporting()
      .withoutMetrics()
      .build())
  }
}
