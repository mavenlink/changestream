package changestream.actors

import akka.actor.SupervisorStrategy.{Escalate, Restart}
import akka.actor.{Actor, ActorRef, ActorRefFactory, OneForOneStrategy}
import akka.pattern.pipe
import com.github.mauricio.async.db.{Configuration, RowData}
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.concurrent.{Await, Future}
import changestream.events.{MutationWithInfo, _}
import com.github.mauricio.async.db.mysql.exceptions.MySQLException
import com.github.mauricio.async.db.mysql.pool.MySQLConnectionFactory
import com.github.mauricio.async.db.pool.{ConnectionPool, PoolConfiguration}

import scala.util.{Failure, Success}

object ColumnInfoActor {
  val COLUMN_NAME = 0
  val DATA_TYPE = 1
  val IS_PRIMARY = 2
  val DATABASE_NAME = 3
  val TABLE_NAME = 4
  val PRELOAD_POSITION = 0

  case class PendingMutation(schemaSequence: Long, event: MutationWithInfo)
}

class ColumnInfoActor (
    getNextHop: ActorRefFactory => ActorRef,
    config: Config = ConfigFactory.load().getConfig("changestream.mysql")
) extends Actor {
  import ColumnInfoActor._
  import context.dispatcher

  override val supervisorStrategy =
    OneForOneStrategy(loggingEnabled = true) {
      case _: MySQLException => Restart
      case _: Exception => Escalate
    }

  protected val log = LoggerFactory.getLogger(getClass)
  protected val nextHop = getNextHop(context)

  protected val PRELOAD_TIMEOUT = config.getLong("preload.timeout")
  protected val preloadDatabases = config.getString("preload.databases")

  protected val TIMEOUT = config.getLong("timeout")
  protected val mysqlConfig = new Configuration(
    config.getString("user"),
    config.getString("host"),
    config.getInt("port"),
    Some(config.getString("password"))
  )
  private val pool = new ConnectionPool(new MySQLConnectionFactory(mysqlConfig), PoolConfiguration.Default)

  // Mutable State
  protected var _schemaSequence = -1
  protected def getNextSchemaSequence: Long = {
    _schemaSequence += 1
    _schemaSequence
  }
  protected val columnsInfoCache = mutable.HashMap.empty[(String, String), ColumnsInfo]
  protected val mutationBuffer = mutable.HashMap.empty[(String, String), List[PendingMutation]]

  override def preStart() = {
    val connectRequest = pool.connect

    connectRequest onComplete {
      case Success(result) =>
        log.info("Connected to MySQL server for Metadata!!")

      case Failure(exception) =>
        log.error("Could not connect to MySQL server for Metadata", exception)
        throw exception
    }

    Await.result(connectRequest, TIMEOUT milliseconds)

    preLoadColumnData
  }

  override def postStop() = {
    Await.result(pool.disconnect, TIMEOUT milliseconds)
  }

  def receive = {
    case event: MutationWithInfo =>
      log.debug(s"Received mutation event on table ${event.mutation.cacheKey}")

      columnsInfoCache.get(event.mutation.cacheKey) match {
        case info: Some[ColumnsInfo] =>
          log.debug(s"Found column info for event on table ${event.mutation.cacheKey}")
          log.debug(s"Adding column info and forwarding to the ${nextHop.path.name} actor")
          nextHop ! event.copy(columns = info)

        case None =>
          log.debug(s"Couldn't find column info for event on table ${event.mutation.cacheKey} -- buffering mutation and kicking off a query")
          val pending = PendingMutation(getNextSchemaSequence, event)
          mutationBuffer(event.mutation.cacheKey) = mutationBuffer.get(event.mutation.cacheKey).fold(List(pending))(buffer =>
            buffer :+ pending
          )

          requestColumnInfo(pending.schemaSequence, event.mutation.database, event.mutation.tableName)
      }

    case columnsInfo: ColumnsInfo =>
      log.debug(s"Received column info for event on table ${columnsInfo.cacheKey}")

      columnsInfoCache(columnsInfo.cacheKey) = columnsInfo

      mutationBuffer.remove(columnsInfo.cacheKey).foreach({ bufferedMutations =>
        // only satisfy mutations that came in after this column info was requested to avoid timing issues with several alters on the same table in quick succession
        val (ready, stillPending) = bufferedMutations.partition(mutation => columnsInfo.schemaSequence <= mutation.schemaSequence)
        mutationBuffer.put(columnsInfo.cacheKey, stillPending)

        if(ready.size > 0) {
          log.debug(s"Adding column info and forwarding ${ready.size} mutations to the ${nextHop.path.name} actor")
          ready.foreach(nextHop ! _.event.copy(columns = Some(columnsInfo)))
        }
      })

    case alter: AlterTableEvent =>
      log.debug(s"Refreshing the cache due to alter table (${alter.cacheKey}): ${alter.sql}")

      columnsInfoCache.remove(alter.cacheKey)
      requestColumnInfo(getNextSchemaSequence, alter.database, alter.tableName)

    case _ =>
      log.error("Invalid message received by ColumnInfoActor")
      throw new Exception("Invalid message received by ColumnInfoActor")
  }

  protected def requestColumnInfo(schemaSequence: Long, database: String, tableName: String) = {
    getColumnsInfo(schemaSequence, database, tableName) recover {
      case exception =>
        log.error(s"Couldn't fetch column info for ${database}.${tableName}", exception)
        throw exception
    } map {
      case Some(result) => result
      case None => log.warn(s"No column metadata found for table ${database}.${tableName}") // should probably fail
    } pipeTo self
  }

  protected def getColumnsInfo(schemaSequence: Long, database: String, tableName: String): Future[Option[ColumnsInfo]] = {
    val escapedDatabase = database.replace("'", "\\'")
    val escapedTableName = tableName.replace("'", "\\'")
    pool
      .sendQuery(s"""
        | select
        |   col.COLUMN_NAME,
        |   col.DATA_TYPE,
        |   case when pk.COLUMN_NAME is not null then true else false end as IS_PRIMARY
        | from INFORMATION_SCHEMA.COLUMNS col
        | left outer join INFORMATION_SCHEMA.KEY_COLUMN_USAGE pk
        |   on col.TABLE_SCHEMA = pk.TABLE_SCHEMA
        |   and col.TABLE_NAME = pk.TABLE_NAME
        |   and col.COLUMN_NAME = pk.COLUMN_NAME
        | where col.TABLE_SCHEMA = '${escapedDatabase}'
        |   and col.TABLE_NAME = '${escapedTableName}'
      """.stripMargin)
      .map(_.rows.map(_.map(getColumnForRow)) match {
        case Some(list) if !list.isEmpty =>
          Some(ColumnsInfo(
            schemaSequence,
            database,
            tableName,
            list
          ))
        case _ =>
          None
      })
  }

  protected def preLoadColumnData = {
    if (!preloadDatabases.isEmpty) {
      val request = for {
        columnsInfo <- getAllColumnsInfo recover {
          case exception =>
            log.error(s"Couldn't fetch metadata for databases: ${preloadDatabases}", exception)
            throw exception
        }
      } yield columnsInfo.foreach {
        case table: ColumnsInfo =>
          columnsInfoCache(table.cacheKey) = table
      }

      Await.result(request, PRELOAD_TIMEOUT milliseconds)
    }
  }

  protected def getAllColumnsInfo: Future[Iterable[ColumnsInfo]] = {
    val databases = preloadDatabases.replace("'", "\'").split(",").mkString("','")

    pool
      .sendQuery(s"""
        | select
        |   lower(col.COLUMN_NAME),
        |   lower(col.DATA_TYPE),
        |   case when pk.COLUMN_NAME is not null then true else false end as IS_PRIMARY,
        |   lower(col.TABLE_SCHEMA) as DATABASE_NAME,
        |   lower(col.TABLE_NAME)
        | from INFORMATION_SCHEMA.COLUMNS col
        | left outer join INFORMATION_SCHEMA.KEY_COLUMN_USAGE pk
        |   on col.TABLE_SCHEMA = pk.TABLE_SCHEMA
        |   and col.TABLE_NAME = pk.TABLE_NAME
        |   and col.COLUMN_NAME = pk.COLUMN_NAME
        | where col.TABLE_SCHEMA in ('${databases}')
      """.stripMargin)
      .map(queryResult =>
        queryResult.rows.map(
          _.groupBy(row =>
            (
              row(DATABASE_NAME).asInstanceOf[String],
              row(TABLE_NAME).asInstanceOf[String]
            )) collect {
            case ((database, table), rows) =>
              ColumnsInfo(
                getNextSchemaSequence,
                database,
                table,
                rows.map(getColumnForRow)
              )
          }
        ).getOrElse(Iterable.empty))
  }

  protected def getColumnForRow(row: RowData): Column = {
    Column(
      name = row(COLUMN_NAME).asInstanceOf[String],
      dataType = row(DATA_TYPE).asInstanceOf[String],
      isPrimary = row(IS_PRIMARY) match {
        case 0 => false
        case 1 => true
      }
    )
  }
}
