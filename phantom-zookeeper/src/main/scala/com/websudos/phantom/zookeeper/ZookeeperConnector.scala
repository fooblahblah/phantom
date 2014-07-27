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

import com.datastax.driver.core.Session
import com.twitter.util.Try

trait ZookeeperConnector {


  protected[zookeeper] val envString = "TEST_ZOOKEEPER_CONNECTOR"

  protected[this] val defaultAddress = new InetSocketAddress("localhost", 2181)

  val zkPath = "/cassandra"

  def zkAddress: InetSocketAddress

  val zkManager: ZookeeperManager

  val keySpace: String

  private[zookeeper] def connectorString = s"${zkAddress.getHostName}:${zkAddress.getPort}"

  def hostnamePortPairs: Seq[InetSocketAddress]

  implicit lazy val session: Session = blocking {
    val s = zkManager.cluster.connect()
    s.execute(s"CREATE KEYSPACE IF NOT EXISTS $keySpace WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};")
    s.execute(s"use $keySpace;")
    s
  }

}

trait DefaultZookeeperConnector extends ZookeeperConnector {

  val zkManager = new DefaultZookeeperManager(this)

  lazy val zkAddress: InetSocketAddress = if (System.getProperty(envString) != null) {
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

  def hostnamePortPairs: Seq[InetSocketAddress] = zkManager.hostnamePortPairs

}
