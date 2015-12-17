package com.datastax.spark.connector.writer

import java.net.InetAddress
import java.util.UUID

import com.datastax.spark.connector.ColumnRef
import com.datastax.spark.connector.cql.TableDef
import org.apache.spark.sql.Row
import com.datastax.spark.connector.types.{InetType, UUIDType, VarIntType, ColumnType}


/** A [[RowWriter]] that can write SparkSQL `Row` objects. */
class SqlRowWriter(val table: TableDef, val selectedColumns: IndexedSeq[ColumnRef])
  extends RowWriter[Row] {

  override val columnNames = selectedColumns.map(_.columnName)

  private val columns = columnNames.map(table.columnByName)
  private val columnTypes = columns.map(_.columnType)
  private val converters = columns.map(_.columnType.converterToCassandra)

  /** Extracts column values from `data` object and writes them into the given buffer
    * in the same order as they are listed in the columnNames sequence. */
  override def readColumnValues(row: Row, buffer: Array[Any]) = {
    require(row.size == columnNames.size, s"Invalid row size: ${row.size} instead of ${columnNames.size}.")
    for (i <- 0 until row.size) {
      buffer(i) = columnTypes(i) match {
        case VarIntType => row(i) match {
          case bigDecimal: java.math.BigDecimal => bigDecimal.toBigInteger
          case bigInteger: java.math.BigInteger => bigInteger
        }
        case UUIDType => if (row(i) == null) { null } else { UUID.fromString(row(i).asInstanceOf[String]) }
        case InetType => InetAddress.getByName(row(i).asInstanceOf[String])
        case other: ColumnType[_] => other.converterToCassandra.convert(row(i))
      }
    }

  }

}


object SqlRowWriter {

  object Factory extends RowWriterFactory[Row] {
    override def rowWriter(table: TableDef, selectedColumns: IndexedSeq[ColumnRef]) =
      new SqlRowWriter(table, selectedColumns)
  }

}
