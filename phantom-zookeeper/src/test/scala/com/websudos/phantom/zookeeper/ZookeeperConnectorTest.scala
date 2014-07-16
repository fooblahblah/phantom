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
import org.scalatest.{ BeforeAndAfterAll, FlatSpec, Matchers }

import com.newzly.util.testing.AsyncAssertionsHelper._

object TestTable extends DefaultZookeeperConnector

class ZookeeperConnectorTest extends FlatSpec with Matchers with BeforeAndAfterAll {
  val instance = new ZookeeperInstance()

  it should "correctly use the default localhost:2181 connector address if no environment variable has been set" in {
    System.setProperty(TestTable.envString, "")

    TestTable.zkAddress.getHostName shouldEqual "localhost"

    TestTable.zkAddress.getPort shouldEqual 2181

  }

  it should "use the values from the environment variable if they are set" in {
    System.setProperty(TestTable.envString, "localhost:4902")

    TestTable.zkAddress.getHostName shouldEqual "localhost"

    TestTable.zkAddress.getPort shouldEqual 4902
  }

  it should "return the default if the environment property is in invalid format" in {

    System.setProperty(TestTable.envString, "localhost:invalidint")

    TestTable.zkAddress.getHostName shouldEqual "localhost"

    TestTable.zkAddress.getPort shouldEqual 2181
  }

  it should "correctly retrieve the Cassandra series of ports from the Zookeeper cluster" in {
    instance.start()

    instance.richClient.getData(TestTable.zkPath, watch = false) successful {
      res => {
        info("Ports correctly retrieved from Cassandra.")
        new String(res.data) shouldEqual "localhost:9142"
      }
    }
  }

  it should "match the Zookeeper connector string to the spawned instance settings" in {
    instance.start()
    TestTable.connectorString shouldEqual instance.zookeeperConnectString
  }

  it should "correctly retrieve the Sequence of InetSocketAddresses from zookeeper" in {
    val pairs = TestTable.hostnamePortPairs

    TestTable.client.getData(TestTable.zkPath, watch = false).successful {
      res => {
        val data = new String(res.data)
        data shouldEqual "localhost:9142"
        Console.println(pairs)
        pairs shouldEqual Seq(new InetSocketAddress("localhost", 9142))
      }
    }
  }

}
