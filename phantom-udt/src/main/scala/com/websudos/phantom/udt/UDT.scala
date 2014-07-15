package com.websudos.phantom.udt

import java.net.InetAddress
import java.util.{ Date, UUID }

import scala.collection.mutable.{ArrayBuffer => MutableArrayBuffer, SynchronizedBuffer => MutableSyncBuffer}
import scala.reflect.runtime.universe.Symbol
import scala.reflect.runtime.{currentMirror => cm, universe => ru}
import scala.util.DynamicVariable
import org.joda.time.DateTime

import com.datastax.driver.core.{ Cluster, Row, UserType }
import com.websudos.phantom.CassandraPrimitive
import com.websudos.phantom.column.AbstractColumn

/**
 * A global lock for reflecting and collecting fields inside a User Defined Type.
 * This prevents a race condition and bug.
 */
private[phantom] object Lock

/**
 * A field part of a user defined type.
 * @param owner The UDT column that owns the field.
 * @tparam T The Scala type corresponding the underlying Cassandra type of the UDT field.
*/
sealed abstract class AbstractField[@specialized(Int, Double, Float, Long, Boolean, Short) T](owner: UDT[_]) {
  lazy val name: String = getClass.getSimpleName.replaceAll("\\$+", "").replaceAll("(anonfun\\d+.+\\d+)|", "")

  protected[this] lazy val valueBox = new DynamicVariable[Option[T]](None)

  def value: T = valueBox.value.getOrElse(null.asInstanceOf[T])

  def cassandraType: String

  def getValue(row: Row): T
}


abstract class Field[Owner <: UDT[_], T : CassandraPrimitive](column: Owner) extends AbstractField[T](column) {
  def apply(item: T): Owner = {
    valueBox.value_=(Some(item))
    column
  }

  val cassandraType = implicitly[CassandraPrimitive[T]].cassandraType
}


abstract class UDT[T] extends AbstractColumn[T] {

  private[this] lazy val _fields: MutableArrayBuffer[AbstractField[_]] = new MutableArrayBuffer[AbstractField[_]] with MutableSyncBuffer[AbstractField[_]]

  val keySpace: String

  val cluster: Cluster

  lazy val typeDef: UserType = cluster.getMetadata.getKeyspace(keySpace).getUserType(name)

  def apply(row: Row): T = {
    _fields.map(_.getValue(row))
  }

  private[this] lazy val _name: String = {
    getClass.getName.split("\\.").toList.last.replaceAll("[^$]*\\$\\$[^$]*\\$[^$]*\\$|\\$\\$[^\\$]*\\$", "").dropRight(1)
  }

  def toCType(v: T): AnyRef = ???

  val cassandraType = _name.toLowerCase

  private[this] val instanceMirror = cm.reflect(this)
  private[this] val selfType = instanceMirror.symbol.toType

  // Collect all column definitions starting from base class
  private[this] val columnMembers = MutableArrayBuffer.empty[Symbol]

  Lock.synchronized {
    selfType.baseClasses.reverse.foreach {
      baseClass =>
        val baseClassMembers = baseClass.typeSignature.members.sorted
        val baseClassColumns = baseClassMembers.filter(_.typeSignature <:< ru.typeOf[AbstractField[_]])
        baseClassColumns.foreach(symbol => if (!columnMembers.contains(symbol)) columnMembers += symbol)
    }

    columnMembers.foreach {
      symbol =>
        val column = instanceMirror.reflectModule(symbol.asModule).instance
        _fields += column.asInstanceOf[AbstractField[_]]
    }
  }

  def schema(): String = {
    val queryInit = s"CREATE TYPE IF NOT EXISTS $cassandraType ("
    val queryColumns = _fields.foldLeft("")((qb, c) => {
        s"$qb, ${c.name} ${c.cassandraType}"
    })
    queryInit + queryColumns + """");""""
  }
}

class StringField[T <: UDT[_]](column: T) extends Field[T, String](column) {
  def getValue(row: Row): String = row.getString(this.name)
}

class InetField[T <: UDT[_]](column: T)  extends Field[T, InetAddress](column) {
  def getValue(row: Row): InetAddress = row.getInet(this.name)
}

class IntField[T <: UDT[_]](column: T)  extends Field[T, Int](column) {
  def getValue(row: Row): Int = row.getInt(this.name)
}

class DoubleField[T <: UDT[_]](column: T)  extends Field[T, Double](column) {
  def getValue(row: Row): Double = row.getDouble(this.name)
}

class LongField[T <: UDT[_]](column: T)  extends Field[T, Long](column) {
  def getValue(row: Row): Long = row.getLong(this.name)
}

class BigIntField[T <: UDT[_]](column: T)  extends Field[T, BigInt](column) {
  def getValue(row: Row): BigInt = row.getVarint(this.name)
}

class BigDecimalField[T <: UDT[_]](column: T)  extends Field[T, BigDecimal](column) {
  def getValue(row: Row): BigDecimal = row.getDecimal(this.name)
}

class DateField[T <: UDT[_]](column: T) extends Field[T, Date](column) {
  def getValue(row: Row): Date = row.getDate(this.name)
}

class DateTimeField[T <: UDT[_]](column: T) extends Field[T, DateTime](column) {
  def getValue(row: Row): DateTime = new DateTime(row.getDate(this.name))
}

//class UDTField[Owner <: UDT[Owner], T <: UDT[_]](column: Owner) extends Field[Owner, T](column)
class UUIDField[Owner <: UDT[_]](column: Owner) extends Field[Owner, UUID](column) {
  def getValue(row: Row): UUID = row.getUUID(this.name)
}
