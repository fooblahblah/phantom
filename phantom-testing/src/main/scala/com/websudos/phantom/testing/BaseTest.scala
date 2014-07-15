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

import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._
import scala.concurrent.{ blocking, ExecutionContext }
import scala.concurrent.ExecutionContext.Implicits.global

import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.scalatest.{ Assertions, BeforeAndAfterAll, FeatureSpec, FlatSpec, Matchers }
import org.scalatest.concurrent.{ AsyncAssertions, ScalaFutures }

import com.datastax.driver.core.{ Cluster, Session }

import com.twitter.conversions.time._
import com.twitter.util.{ Await, Try }
import com.websudos.phantom.zookeeper.{DefaultZookeeperConnector, ZookeeperInstance}

object BaseTestHelper {

  val zkInstance = new ZookeeperInstance()

  zkInstance.start()
  val embeddedMode = new AtomicBoolean(false)

  private[this] def getPort: Int = {
    if (System.getenv().containsKey("TRAVIS_JOB_ID")) {
      Console.println("Using Cassandra as a Travis Service with port 9042")
      9142
    } else {
      Console.println("Using Embedded Cassandra with port 9142")
      embeddedMode.compareAndSet(false, true)
      9142
    }
  }

  val ports = Try {
    val defaults = new String(Await.result(zkInstance.richClient.getData("/cassandra", watch = false), 3.seconds).data)
    val seq = defaults.split("\\s*,\\s*").map(_.split(":")) map {
      case Array(hostname, port) => new InetSocketAddress(hostname, port.toInt)
    }
    seq.toSeq
  } getOrElse Seq.empty[InetSocketAddress]

  val cluster = Cluster.builder()
    .addContactPointsWithPorts(ports.asJava)
    .withoutJMXReporting()
    .withoutMetrics()
    .build()

  lazy val session = blocking {
    cluster.connect()
  }
}

trait CassandraTest {
  self: BeforeAndAfterAll =>

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
    if (BaseTestHelper.embeddedMode.get()) {
      Try {
        EmbeddedCassandraServerHelper.mkdirs()
      }
      EmbeddedCassandraServerHelper.startEmbeddedCassandra("cassandra.yaml")
    }
    createKeySpace(keySpace)
  }
}

trait BaseTest extends FlatSpec with ScalaFutures with BeforeAndAfterAll with Matchers with Assertions with AsyncAssertions with CassandraTest with DefaultZookeeperConnector

trait FeatureBaseTest extends FeatureSpec with ScalaFutures with BeforeAndAfterAll with Matchers with Assertions with AsyncAssertions with CassandraTest with DefaultZookeeperConnector


