/*
 *
 *  * Copyright 2014 websudos ltd.
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

package com.websudos.phantom.testing

import java.net.ServerSocket
import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global

import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.scalatest.concurrent.{AsyncAssertions, ScalaFutures}
import org.scalatest.{Assertions, BeforeAndAfterAll, FeatureSpec, FlatSpec, Matchers}

import com.twitter.util.{NonFatal, Try}
import com.websudos.phantom.zookeeper.{ZookeeperInstance, ZookeeperManager => Manager}


private[testing] object ZookeperManager {
  lazy val zkInstance = new ZookeeperInstance()

  private[this] val isStarted = new AtomicBoolean(false)

  def start(): Unit = {
    if (isStarted.compareAndSet(false, true)) {
      zkInstance.start()
    }
  }

  /**
   * This does a dummy check to see if Cassandra is started.
   * It checks for default ports for embedded Cassandra and local Cassandra.
   * @return A boolean saying if Cassandra is started.
   */
  def isCassandraStarted: Boolean = {
    Try { new ServerSocket(9142) }.toOption.isEmpty
  }
}

trait CassandraTest extends ScalaFutures with Matchers with Assertions with AsyncAssertions {
  self: BeforeAndAfterAll =>

  ZookeperManager.start()

  implicit lazy val context: ExecutionContext = global

  override def beforeAll() {
    if (!ZookeperManager.isCassandraStarted) {
      try {
        EmbeddedCassandraServerHelper.mkdirs()
      } catch {
        case NonFatal(e) => Manager.logger.error(e.getMessage)
      }
      EmbeddedCassandraServerHelper.startEmbeddedCassandra("cassandra.yaml")
    }
  }
}

trait BaseTest extends FlatSpec with BeforeAndAfterAll with CassandraTest

trait FeatureBaseTest extends FeatureSpec with BeforeAndAfterAll with CassandraTest


