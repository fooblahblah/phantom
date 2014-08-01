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

import scala.concurrent.blocking
import scala.util.DynamicVariable

import org.scalatest.concurrent.{AsyncAssertions, ScalaFutures}
import org.scalatest.{Assertions, BeforeAndAfterAll, Matchers, Suite}

import com.datastax.driver.core.{Cluster, Session}

trait CassandraManager {
  val cluster: Cluster
  implicit def session: Session
}


object SimpleCassandraManager extends CassandraManager {

  private[this] lazy val sessionStore = new DynamicVariable[Session](null)
  private[this] val inited = false

  lazy val cluster: Cluster = Cluster.builder()
    .addContactPoint("localhost")
    .withPort(9142)
    .withoutJMXReporting()
    .withoutMetrics()
    .build()

  implicit def session = sessionStore.value

  def initIfNotInited(keySpace: String): Unit = synchronized {
    if (!inited) {
      sessionStore.value_=(
        blocking {
          cluster.connect(keySpace)
        }
      )
    }
  }

}


trait SimpleCassandraConnector extends CassandraSetup {
  val keySpace: String
}

trait SimpleCassandraTest extends ScalaFutures with SimpleCassandraConnector with Matchers with Assertions with AsyncAssertions with BeforeAndAfterAll {
  self : BeforeAndAfterAll with Suite =>

  override def beforeAll() {
    super.beforeAll()
    setupCassandra()
    SimpleCassandraManager.initIfNotInited(keySpace)
  }
}
