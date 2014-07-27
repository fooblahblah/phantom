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

import com.datastax.driver.core.{Cluster, Session}
import com.twitter.conversions.time._
import com.twitter.finagle.exp.zookeeper.ZooKeeper
import com.twitter.util.{Await, Try}

trait ZookeeperConnector {

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
      zkManager.logger.info("Connecting to Zookeeper instance")
      Await.ready(client.connect(2.seconds), 2.seconds)
      connectionStatus = true
    } else {
      zkManager.logger.info("Already connected to Zookeeper instance")
    }
  }


  protected[zookeeper] val envString = "TEST_ZOOKEEPER_CONNECTOR"

  protected[this] val defaultAddress = new InetSocketAddress("localhost", 2181)

  val zkPath = "/cassandra"

  def zkAddress: InetSocketAddress

  val zkManager: ZookeeperManager

  val keySpace: String

  lazy val cluster: Cluster = zkManager.cluster

  private[zookeeper] def connectorString = s"${zkAddress.getHostName}:${zkAddress.getPort}"

  lazy val client = ZooKeeper.newRichClient(connectorString)

  def hostnamePortPairs: Seq[InetSocketAddress]

  implicit lazy val session: Session = blocking {
    val s = cluster.connect()
    s.execute(s"CREATE KEYSPACE IF NOT EXISTS $keySpace WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};")
    s.execute(s"use $keySpace;")
    s
  }

}

trait DefaultZookeeperConnector extends ZookeeperConnector {

  val zkManager = new DefaultZookeeperManager(this)

  def zkAddress: InetSocketAddress = if (System.getProperty(envString) != null) {
    val inetPair: String = System.getProperty(envString)
    val split = inetPair.split(":")

    Try {
      zkManager.logger.info(s"Using ZooKeeper settings from the $envString environment variable")
      zkManager.logger.info(s"Connecting to ZooKeeper address: ${split(0)}:${split(1)}")
      new InetSocketAddress(split(0), split(1).toInt)
    } getOrElse {
      zkManager.logger.warn(s"Failed to parse address from $envString environment variable with value: $inetPair")
      defaultAddress
    }
  } else {
    zkManager.logger.info(s"No custom settings for Zookeeper found in $envString. Using localhost:2181 as default.")
    defaultAddress
  }

  def hostnamePortPairs: Seq[InetSocketAddress] = Try {
    connectIfNotConnected()
    val res = new String(Await.result(client.getData(zkPath, watch = false), 3.seconds).data)
    zkManager.logger.info("Extracting Cassandra ports from ZooKeeper")
    zkManager.logger.info(s"Parsing from $res")

    res.split("\\s*,\\s*").map(_.split(":")).map {
      case Array(hostname, port) => new InetSocketAddress(hostname, port.toInt)
    }.toSeq

  } getOrElse Seq.empty[InetSocketAddress]

}
