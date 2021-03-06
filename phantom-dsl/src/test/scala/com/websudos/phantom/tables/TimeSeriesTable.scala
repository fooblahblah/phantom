/*
 * Copyright 2013 websudos ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.websudos.phantom.tables

import java.util.UUID
import org.joda.time.DateTime
import com.datastax.driver.core.Row
import com.datastax.driver.core.utils.UUIDs
import com.websudos.phantom.helper.{ ModelSampler, TestSampler }
import com.websudos.phantom.Implicits._
import com.newzly.util.testing.Sampler

case class TimeSeriesRecord(
  id: UUID,
  name: String,
  timestamp: DateTime
)

object TimeSeriesRecord extends ModelSampler[TimeSeriesRecord] {
  val testUUID = UUIDs.timeBased()
  def sample: TimeSeriesRecord = {
    TimeSeriesRecord(
      testUUID,
      Sampler.getARandomString,
      new DateTime()
    )
  }
}

sealed class TimeSeriesTable extends CassandraTable[TimeSeriesTable, TimeSeriesRecord] {
  object id extends UUIDColumn(this) with PartitionKey[UUID]
  object name extends StringColumn(this)
  object timestamp extends DateTimeColumn(this) with ClusteringOrder[DateTime] with Descending

  def fromRow(row: Row): TimeSeriesRecord = {
    TimeSeriesRecord(
      id(row),
      name(row),
      timestamp(row)
    )
  }
}

object TimeSeriesTable extends TimeSeriesTable with TestSampler[TimeSeriesTable, TimeSeriesRecord]
