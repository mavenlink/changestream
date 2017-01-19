package changestream

import akka.actor.{Actor, ActorRef, ActorRefFactory, ActorSystem, Props}
import changestream.actors._
import changestream.events._
import com.github.shyiko.mysql.binlog.event._
import com.github.shyiko.mysql.binlog.BinaryLogClient.EventListener
import com.github.shyiko.mysql.binlog.event.EventType._
import org.slf4j.LoggerFactory
import com.typesafe.config.{Config, ConfigFactory}

object ChangeStreamEventListener extends EventListener {
  protected val log = LoggerFactory.getLogger(getClass)
  protected val system = ActorSystem("changestream")

  protected val systemDatabases = Seq("information_schema", "mysql", "performance_schema", "sys")
  protected val whitelist: java.util.List[String] = new java.util.LinkedList[String]()
  protected val blacklist: java.util.List[String] = new java.util.LinkedList[String]()

  protected val binlogActor = system.actorOf(Props(new BinlogClient), name = "binlogActor")
  protected val emitterCoordinator = system.actorOf(Props(new EmitterCoordinator(_ => binlogActor, getEmitterTopic)), name = "emitterCoordinator")
  @volatile protected var emitterLoader: (ActorRefFactory => ActorRef) = (_ => emitterCoordinator)
  @volatile protected var _emitterTopic: String = "firehose"
  @volatile protected var _currentBinlogFile: String = ""

  protected val formatterActor = system.actorOf(Props(new JsonFormatterActor(emitterLoader)), name = "formatterActor")
  protected val columnInfoActor = system.actorOf(Props(new ColumnInfoActor(_ => formatterActor)), name = "columnInfoActor")
  protected val transactionActor = system.actorOf(Props(new TransactionActor(_ => columnInfoActor)), name = "transactionActor")

  setConfig(ConfigFactory.load().getConfig("changestream"))

  /** Allows the configuration for the listener object to be set on startup.
    * The listener will look for whitelist, blacklist, and emitter settings.
    *
    * @param config
    */
  def setConfig(config: Config) = {
    whitelist.clear()
    blacklist.clear()

    if(config.hasPath("whitelist")) {
      config.getString("whitelist").split(',').foreach(whitelist.add(_))
      log.info(s"Using event whitelist: ${whitelist}")
    }
    else if(config.hasPath("blacklist")) {
      config.getString("blacklist").split(',').foreach(blacklist.add(_))
      log.info(s"Using event blacklist: ${blacklist}")
    }

    _emitterTopic = config.getString("emitter-topic")
  }

  /** Allows the emitter/producer actor to be configured at runtime.
    *
    * Note: you must initialize the emitter before the first event arrives, or it will be ignored
    *
    * @param loader A lamda that returns your actor ref
    */
  def setEmitterLoader(loader: (ActorRefFactory => ActorRef)) = emitterLoader = loader

  /**
    * Lazy load the emitter topic (gets called after setconfig)
    * @return
    */
  def getEmitterTopic = _emitterTopic
  def currentBinlogFile = _currentBinlogFile

  /**
    * TODO
    */
//  def currentPosition: Option[BinlogPosition] = {
//    //client.getGtidSet match {
//      //case null =>
//        currentBinlogFile match { //scalastyle:ignore
//          case null => None //scalastyle:ignore
//          case file: String => {
//            val position = nextPosition match {
//              case 0 => client.getBinlogPosition
//              case _ => nextPosition
//            }
//
//            Some(FilePosition(file, position))
//          }
//        }
//      //case gtid: String => Some(GtidPosition(gtid))
//    //}
//  }

  /** Sends binlog events to the appropriate changestream actor.
    *
    * @param binaryLogEvent The binlog event
    */
  def onEvent(binaryLogEvent: Event) = {
    log.debug(s"Received event: ${binaryLogEvent}")
    val header = binaryLogEvent.getHeader[EventHeaderV4]
    val changeEvent = getChangeEvent(binaryLogEvent, header)

    changeEvent match {
      case Some(e: TransactionEvent) =>
        transactionActor ! e

      case Some(e: MutationEvent) =>
        val position = FilePosition(currentBinlogFile, header.getNextPosition)
        transactionActor ! MutationWithInfo(e, position = Some(position))

      case Some(e: AlterTableEvent) =>
        columnInfoActor ! e

      case None =>
        log.debug(s"Ignoring ${header.getEventType} event.")
    }
  }

  /** Returns the appropriate ChangeEvent case object given a binlog event object.
    *
    * @param event The java binlog listener event
    * @return The resulting ChangeEvent
    */
  def getChangeEvent(event: Event, header: EventHeaderV4): Option[ChangeEvent] = {
    header.getEventType match {
      case eventType if EventType.isRowMutation(eventType) =>
        getMutationEvent(event, header)

      case GTID =>
        Some(Gtid(event.getData[GtidEventData].getGtid))

      case XID =>
        Some(CommitTransaction(header.getNextPosition))

      case QUERY =>
        parseQueryEvent(event.getData[QueryEventData], header)

      case FORMAT_DESCRIPTION =>
        val data = event.getData[FormatDescriptionEventData]
        log.info(s"Server version: ${data.getServerVersion}, binlog version: ${data.getBinlogVersion}")
        None

      case ROTATE =>
        _currentBinlogFile = event.getData[RotateEventData].getBinlogFilename
        None

      // Known events that are safe to ignore
      case PREVIOUS_GTIDS => None
      case ROWS_QUERY => None
      case TABLE_MAP => None
      case ANONYMOUS_GTID => None
      case STOP => None

      case _ =>
        val message = s"Received unknown message: ${event}"
        log.error(message)
        throw new Exception(message)
    }
  }

  protected def getMutationEvent(event: Event, header: EventHeaderV4): Option[MutationEvent] = {
    val mutation = header.getEventType match {
      case e if EventType.isWrite(e) =>
        event.getData[Insert].copy(timestamp = header.getTimestamp)

      case e if EventType.isUpdate(e) =>
        event.getData[Update].copy(timestamp = header.getTimestamp)

      case e if EventType.isDelete(e) =>
        event.getData[Delete].copy(timestamp = header.getTimestamp)
    }

    shouldIgnore(mutation) match {
      case true =>
        log.debug(s"Ignoring event for table ${mutation.database}.${mutation.tableName}.")
        None
      case false =>
        Some(mutation)
    }
  }

  /** Returns a ChangeEvent case class instance representing the change indicated by
    * the given binlog QUERY event (either BEGIN, COMMIT, ROLLBACK, or ALTER...).
    *
    * @param queryData The QUERY event data
    * @return
    */
  protected def parseQueryEvent(queryData: QueryEventData, header: EventHeaderV4): Option[ChangeEvent] = {
    queryData.getSql match {
      case sql if sql matches "(?i)^begin" =>
        Some(BeginTransaction)

      case sql if sql matches "(?i)^commit" =>
        Some(CommitTransaction(header.getNextPosition))

      case sql if sql matches "(?i)^rollback" =>
        Some(RollbackTransaction)

      case sql if sql matches "(?i)^alter.*" =>
        /** Not(space, dot)+ OR backtick + Not(backtick, dot) + backtick OR "Not(", dot)" **/
        /** Does not currently support spaces or backticks in table or db names **/
        val dbOrTableName = "([^\\s\\.]+|`[^`\\.]+`|\"[^\"\\.]\")"
        val identifierRegex = s"(?i)^alter\\s+(ignore\\s+)?table\\s+($dbOrTableName(\\.$dbOrTableName)?).*".r

        var maybeIdentifier:Option[(String, String)] = None
        for { p <- identifierRegex findAllIn sql } p match {
          case identifierRegex(_, _, first, _, second) =>
            maybeIdentifier = Some(second match {
              case null => (queryData.getDatabase, unescapeIdentifier(first)) //scalastyle:ignore
              case _ => (unescapeIdentifier(first), unescapeIdentifier(second))
            })
        }

        maybeIdentifier.map({
          case (db, table) => Some(AlterTableEvent(db.toLowerCase, table.toLowerCase, sql))
        }).getOrElse(
          None
        )
      case sql =>
        None
    }
  }

  protected def unescapeIdentifier(escaped: String) = escaped.charAt(0) match {
    case '`' => escaped.substring(1, escaped.length - 1).replace("``", "`")
    case '"' => escaped.substring(1, escaped.length - 1).replace("\"\"", "\"")
    case _ => escaped
  }

  private def shouldIgnore(info: MutationEvent) = {
    if(systemDatabases.contains(info.database)) {
      true
    }
    else if(!whitelist.isEmpty) {
      tableMissingFromWhitelist(info.database, info.tableName)
    }
    else if(!blacklist.isEmpty) {
      tableInBlacklist(info.database, info.tableName)
    }
    else {
      false
    }
  }

  private def tableInBlacklist(database: String, table: String) = {
    tableInList(database, table, blacklist)
  }

  private def tableMissingFromWhitelist(database: String, table: String) = {
    !tableInList(database, table, whitelist)
  }

  private def tableInList(database: String, table: String, list: java.util.List[String]) = {
    if(list.isEmpty) {
      false
    }
    else if(list.contains(s"${database}.*") || list.contains(s"${database}.${table}")) {
      true
    }
    else {
      false
    }
  }
}
