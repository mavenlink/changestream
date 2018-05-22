package changestream.helpers

import java.io.Serializable
import java.util

import changestream.events._

import scala.collection.immutable.ListMap
import scala.util.Random

object Fixtures {
  val columns = IndexedSeq(
    Column("id", "int", true),
    Column("username", "varchar", false),
    Column("password", "varchar", false),
    Column("login_count", "int", false),
    Column("bio", "text", false),
    Column("two_bit_field", "bit", false),
    Column("float_field", "float", false),
    Column("big_decimal_field", "decimal", false),
    Column("java_util_date", "datetime", false),
    Column("java_sql_date", "date", false),
    Column("java_sql_time", "time", false),
    Column("java_sql_timestamp", "timestamp", false),
    Column("blob_that_should_be_ignored", "blob", false)
  )

  def timestamp: Long = System.currentTimeMillis

  def getColumnsInfo(database: String, tableName: String) = {
    ColumnsInfo(0, database, tableName, columns)
  }

  def insertSql(columnsInfo: ColumnsInfo, rows: Seq[ListMap[String, Any]]): String = {
    val columnList = columnsInfo.columns.map(_.name).mkString(", ")
    val valueList = rows.map(row => {
      val rowDataList = row.map({
        case (k, s: String) => s"'${s}'"
        case (k, x) => x.toString
      }).mkString(", ")

      s"(${rowDataList})"
    }).mkString(", ")
    s"insert into ${columnsInfo.database}.${columnsInfo.tableName} (${columnList}) values ${valueList}"
  }

  def updateSql(columnsInfo: ColumnsInfo, rows: Seq[ListMap[String, Any]]): String = {
    val assignments = columnsInfo.columns.map({
      case (v) =>
        val right = rows(0)(v.name) match {
          case s: String => s"'${s}'"
          case x => x.toString
        }
        s"${v.name} = ${right}"
    }).mkString(", ")

    val randCol = columnsInfo.columns(Random.nextInt(columnsInfo.columns.size)).name

    s"update ${columnsInfo.database}.${columnsInfo.tableName} set ${assignments} where ${randCol} is null"
  }

  def deleteSql(columnsInfo: ColumnsInfo): String = {
    val where = "id = -1 limit 1"
    s"delete from ${columnsInfo.database}.${columnsInfo.tableName} where ${where}"
  }

  private def randomWord(maxLength: Int): String = {
    val alphabet = "abcdefghijklmnopqrstuvwxyz"
    Stream.continually(Random.nextInt(alphabet.size)).map(alphabet).take(maxLength).mkString
  }

  def randomRowData: ListMap[String, Any] = ListMap(
    "id" -> Random.nextInt(99999999),
    "username" -> s"${randomWord(12)} ${randomWord(15)}",
    "password" -> randomWord(16),
    "login_count" -> Random.nextInt(1000),
    "bio" -> (1 to 30).map(idx => randomWord(12)).mkString(Seq(", ", ". ", " ")(Random.nextInt(20) match { case 0 => 0 case 1 => 1 case _ => 2 })),
    "two_bit_field" -> {
      val size = Random.nextInt(63) // valid range is 1-64
      val bits = new util.BitSet(size + 1)
      (0 to size).foreach(idx => bits.set(idx, Random.nextInt(2) match { case 0 => false case 1 => true }))
      bits
    },
    "float_field" -> Random.nextFloat(),
    "big_decimal_field" -> new java.math.BigDecimal(Random.nextDouble()),
    "java_util_date" -> new java.util.Date(),
    "java_sql_date" -> new java.sql.Date(new java.util.Date().getTime),
    "java_sql_time" -> new java.sql.Time(new java.util.Date().getTime),
    "java_sql_timestamp" -> new java.sql.Timestamp(new java.util.Date().getTime),
    "blob_that_should_be_ignored" -> {
      val bytes = new Array[Byte](32)
      Random.nextBytes(bytes)
      bytes
    }
  )

  def mutation(
                mutationType: String,
                rowCount: Int = 1,
                rowsInTransaction: Int = 1,
                sequenceNext: Long = 0,
                database: String = "changestream_test",
                tableName: String = "users",
                tableId: Int = 123
              ): (MutationEvent, Seq[ListMap[String, Any]], Seq[ListMap[String, Any]]) = {
    val rowsData = (1 to rowCount).map(idx => randomRowData)
    val rowsDataOld = (1 to rowCount).map(idx => randomRowData)

    val includedColumns = new util.BitSet()
    includedColumns.set(0, 13) // Remember to add to this number if you add test columns

    val mutation = mutationType match {
      case "insert" => {
        Insert(tableId,
          includedColumns,
          rawBinlogData(rowsData),
          database,
          tableName,
          Some(insertSql(getColumnsInfo(database, tableName), rowsData)),
          sequenceNext,
          timestamp
        )
      }
      case "update" => {
        Update(
          tableId,
          includedColumns,
          includedColumns,
          rawBinlogData(rowsData),
          rawBinlogData(rowsDataOld),
          database,
          tableName,
          Some(updateSql(getColumnsInfo(database, tableName), rowsData)),
          sequenceNext,
          timestamp)
      }
      case "delete" => {
        Delete(
          tableId,
          includedColumns,
          rawBinlogData(rowsData),
          database,
          tableName,
          Some(deleteSql(getColumnsInfo(database, tableName))),
          sequenceNext,
          timestamp
        )
      }
    }

    (mutation, rowsData, rowsDataOld)
  }

  def rawBinlogData(rowsData: Seq[ListMap[String, Any]]) = {
    rowsData.map(rawBinlogRowData(_)).toList
  }

  def rawBinlogRowData(rowData: ListMap[String, Any]) = {
    rowData.zipWithIndex.map({
      case ((k, v), idx) =>
        v.asInstanceOf[Serializable]
    }).toArray
  }

  def transactionInfo(rowsInTransaction: Int = 1, isLastChangeInTransaction: Boolean = false) =
    TransactionInfo(
      gtid = java.util.UUID.randomUUID().toString,
      currentRow = rowsInTransaction,
      lastMutationInTransaction = isLastChangeInTransaction
    )
  def transactionInfoGtid(rowsInTransaction: Int = 1, isLastChangeInTransaction: Boolean = false) =
    TransactionInfo(
      gtid = s"${java.util.UUID.randomUUID().toString}:${Random.nextInt(100000)}",
      currentRow = rowsInTransaction,
      lastMutationInTransaction = isLastChangeInTransaction
    )
  def transactionInfoEither(rowsInTransaction: Int = 1, isLastChangeInTransaction: Boolean = false) = Random.nextInt(2) match {
    case 0 => transactionInfo(rowsInTransaction, isLastChangeInTransaction)
    case 1 => transactionInfoGtid(rowsInTransaction, isLastChangeInTransaction)
  }

  def mutationWithInfo(
                        mutationType: String,
                        rowCount: Int = 1,
                        transactionCurrentRow: Int = 1,
                        transactionInfo: Boolean = true,
                        columns: Boolean = true,
                        sequenceNext: Long = 0,
                        database: String = "changestream_test",
                        tableName: String = "users",
                        tableId: Int = 123,
                        isLastChangeInTransaction: Boolean = false
                      ): (MutationWithInfo, Seq[ListMap[String, Any]], Seq[ListMap[String, Any]]) = {
    val (m, d, od) = mutation(mutationType, rowCount, transactionCurrentRow, sequenceNext, database, tableName, tableId)
    (MutationWithInfo(
      m,
      transaction = transactionInfo match {
        case true => Some(transactionInfoEither(transactionCurrentRow, isLastChangeInTransaction))
        case false => None
      },
      columns = columns match {
        case true => Some(getColumnsInfo(database, tableName))
        case false => None
      }
    ), d, od)
  }
}
