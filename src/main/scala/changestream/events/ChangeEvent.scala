package changestream.events

import com.github.shyiko.mysql.binlog.event._

import java.util

/** Parent trait for all change events. Change event are an
  * abstraction of mysql binlog events. They are named to be
  * easily understood in the context of changestream, and
  * include data that is required for the payload creation.
  */
sealed trait ChangeEvent

/** Container for information about a transaction event.
  *
  * Can be created from binlog QUERY (BEGIN/COMMIT/ROLLBACK),
  * and XID events.
  *
  * https://dev.mysql.com/doc/internals/en/query-event.html
  *
  */
sealed trait TransactionEvent extends ChangeEvent
case object BeginTransaction extends TransactionEvent
case class Gtid(gtid: String) extends TransactionEvent
case object CommitTransaction extends TransactionEvent
case object RollbackTransaction extends TransactionEvent

/** Represents an ALTER TABLE statement, created from QUERY
  * event in the binlog.
  *
  * @param database
  * @param tableName
  * @param sql
  */
case class AlterTableEvent(
                            database: String,
                            tableName: String,
                            sql: String
                          ) extends ChangeEvent {
  def cacheKey = (database.toLowerCase, tableName.toLowerCase)
}

/** Represents a mutation event (insert/update/delete) that can include
  * one or more row changes per event.
  *
  * Created from binlog ROWS_EVENT events.
  *
  * https://dev.mysql.com/doc/internals/en/rows-event.html
  *
  */
sealed trait MutationEvent extends ChangeEvent with EventData {
  def tableId: Long
  def includedColumns: util.BitSet
  def rows: List[Array[java.io.Serializable]]
  def database: String
  def tableName: String
  def sql: Option[String]
  def sequence: Long
  def timestamp: Long
  def cacheKey = (database.toLowerCase, tableName.toLowerCase)
}

/** Row and column information for an Insert mutation.
  */
case class Insert(
                   tableId: Long,
                   includedColumns: util.BitSet,
                   rows: List[Array[java.io.Serializable]],
                   database: String = "",
                   tableName: String = "",
                   sql: Option[String] = None,
                   sequence: Long = 0,
                   timestamp: Long = 0
                 ) extends MutationEvent {
  override def toString = "insert"
}

/** Row and column information for an Update mutation.
  */
case class Update(
                   tableId: Long,
                   includedColumns: util.BitSet,
                   includedColumnsBeforeUpdate: util.BitSet,
                   rows: List[Array[java.io.Serializable]],
                   oldRows: List[Array[java.io.Serializable]],
                   database: String = "",
                   tableName: String = "",
                   sql: Option[String] = None,
                   sequence: Long = 0,
                   timestamp: Long = 0
                 ) extends MutationEvent {
  override def toString = "update"
}

/** Row and column information for a Delete mutation.
  */
case class Delete(
                   tableId: Long,
                   includedColumns: util.BitSet,
                   oldRows: List[Array[java.io.Serializable]],
                   database: String = "",
                   tableName: String = "",
                   sql: Option[String] = None,
                   sequence: Long = 0,
                   timestamp: Long = 0
                 ) extends MutationEvent {
  override def toString = "delete"
}

/** Container for information about a mutation's transaction.
  *
  * @param gtid The unique identifier for the transaction
  * @param currentRow The current row count in transaction
  * @param lastMutationInTransaction Signifies that this mutation is the last in the transaction
  */
case class TransactionInfo(gtid: String, currentRow: Long = 0, lastMutationInTransaction: Boolean = false)

/** Container for a table's column information.
  *
  * @param schemaSequence Sequence number used internally by the ColumnInfoActor
  * @param database
  * @param tableName
  * @param columns
  */
case class ColumnsInfo(
                        schemaSequence: Long,
                        database: String,
                        tableName: String,
                        columns: IndexedSeq[Column]
                      ) {

  /** The values used as a key for the ColumnInfoActor's cache
    *
    * @return Database and Table names.
    */
  def cacheKey = (database.toLowerCase, tableName.toLowerCase)
}

/** Information about a single column in a table
  *
  * @param name The column's name
  * @param dataType The SQL datatype (lowercase string, such as "varchar" or "int")
  * @param isPrimary Is this column the primary key (or part of a multi-column primary key)?
  */
case class Column(name: String, dataType: String, isPrimary: Boolean)

/** Contains a mutation event and all of its associated information.
  *
  * Should contain everything that is needed to format the event payload.
  *
  * @param mutation
  * @param transaction
  * @param columns
  */
case class MutationWithInfo(
                             mutation: MutationEvent,
                             transaction: Option[TransactionInfo] = None,
                             columns: Option[ColumnsInfo] = None,
                             formattedMessage: Option[String] = None
                           )
