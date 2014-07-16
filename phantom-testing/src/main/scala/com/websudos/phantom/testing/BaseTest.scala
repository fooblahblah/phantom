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

import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ExecutionContext, blocking}

import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.scalatest.concurrent.{AsyncAssertions, ScalaFutures}
import org.scalatest.{Assertions, BeforeAndAfterAll, FeatureSpec, FlatSpec, Matchers}

import com.datastax.driver.core.Session
import com.twitter.util.Try
import com.websudos.phantom.zookeeper.{ZookeeperInstance, DefaultZookeeperConnector}


private[testing] object ZookeperManager {
  lazy val zkInstance = new ZookeeperInstance()

  private[this] val isStarted = new AtomicBoolean(false)

  def start(): Unit = {
    if (isStarted.compareAndSet(false, true)) {
      zkInstance.start()
    }
  }
}

trait CassandraTest {
  self: BeforeAndAfterAll =>

  ZookeperManager.start()

  val keySpace: String

  val session: Session

  implicit lazy val context: ExecutionContext = global

  private[this] def createKeySpace(spaceName: String) = {
    blocking {
      session.execute(s"CREATE KEYSPACE IF NOT EXISTS $spaceName WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};")
      session.execute(s"use $spaceName;")
    }
  }

  override def beforeAll() {
    Try {
      EmbeddedCassandraServerHelper.mkdirs()
    }
    EmbeddedCassandraServerHelper.startEmbeddedCassandra("cassandra.yaml")
    createKeySpace(keySpace)
  }
}

trait BaseTest extends FlatSpec with ScalaFutures with BeforeAndAfterAll with Matchers with Assertions with AsyncAssertions with CassandraTest with DefaultZookeeperConnector

trait FeatureBaseTest extends FeatureSpec with ScalaFutures with BeforeAndAfterAll with Matchers with Assertions with AsyncAssertions with CassandraTest with DefaultZookeeperConnector


