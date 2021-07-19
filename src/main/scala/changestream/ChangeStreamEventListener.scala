package changestream

import java.util.concurrent.atomic.AtomicLong

import akka.Done
import akka.actor.{Actor, ActorRef, ActorRefFactory, ActorSystem, CoordinatedShutdown, Props}
import akka.io.IO
import akka.pattern.ask
import akka.util.Timeout
import changestream.actors.PositionSaver._
import changestream.actors._
import changestream.events._

import scala.concurrent.duration._
import scala.language.postfixOps
import com.github.shyiko.mysql.binlog.event._
import com.github.shyiko.mysql.binlog.BinaryLogClient.EventListener
import com.github.shyiko.mysql.binlog.event.EventType._
import org.slf4j.LoggerFactory
import com.typesafe.config.Config
import kamon.Kamon
import spray.can.Http

import scala.concurrent.Future

object ChangeStreamEventListener extends EventListener {
  protected val log = LoggerFactory.getLogger(getClass)
  protected val counterMetric = Kamon.counter("changestream.binlog_event.total")
  protected val inFlightMetric = Kamon.rangeSampler("changestream.in_flight")

  implicit val system = ActorSystem("changestream") // public for tests
  protected implicit val ec = system.dispatcher
  protected implicit val timeout = Timeout(10 seconds)

  protected val systemDatabases = Seq("information_schema", "mysql", "performance_schema", "sys")
  protected val whitelist: java.util.List[String] = new java.util.LinkedList[String]()
  protected val blacklist: java.util.List[String] = new java.util.LinkedList[String]()

  @volatile protected var inFlightLimit: Option[Long] = None
  @volatile protected var inFlightLimitSleepTime: Long = 100
  @volatile protected var inFlightCount: AtomicLong = new AtomicLong(0)
  @volatile protected var positionSaver: Option[ActorRef] = None
  @volatile protected var emitter: Option[ActorRef] = None
  @volatile protected var currentBinlogFile: Option[String] = None
  @volatile protected var currentTransactionPosition: Option[Long] = None
  @volatile protected var currentRowsQueryPosition: Option[Long] = None
  @volatile protected var currentTableMapPosition: Option[Long] = None

  protected lazy val formatterActor = system.actorOf(Props(new JsonFormatterActor(_ => emitter.get)), name = "formatterActor")
  protected lazy val columnInfoActor = system.actorOf(Props(new ColumnInfoActor(_ => formatterActor)), name = "columnInfoActor")
  protected lazy val transactionActor = system.actorOf(Props(new TransactionActor(_ => columnInfoActor)), name = "transactionActor")

  /** Gracefully handle application shutdown from
    *  - Normal program exit
    *  - TERM signal
    *  - System reboot/shutdown
    */
  CoordinatedShutdown(system).addTask(CoordinatedShutdown.PhaseBeforeServiceUnbind, "disconnectBinlogClient") { () =>
    log.info("Initiating shutdown...")

    Future { ChangeStream.disconnectClient }.map(_ => Done)
  }

  CoordinatedShutdown(system).addTask(CoordinatedShutdown.PhaseServiceUnbind, "drainPipeline") { () =>
    import akka.pattern.gracefulStop
    log.info("Draining event pipeline...")

    for {
      _ <- gracefulStop(transactionActor, 5 seconds)
      _ <- gracefulStop(columnInfoActor, 5 seconds)
      _ <- gracefulStop(formatterActor, 5 seconds)
      _ <- emitter match {
        case None =>
          Future { Done }
        case Some(actor) =>
          // stop and wait for futures (i.e. external calls) to complete
          gracefulStop(actor, 30 seconds)
      }
    } yield Done
  }

  CoordinatedShutdown(system).addTask(CoordinatedShutdown.PhaseServiceRequestsDone, "saveLastPosition") { () =>
    import akka.pattern.gracefulStop
    log.info("Saving last position...")

    positionSaver match {
      case None =>
        Future { Done }
      case Some(actor) =>
        gracefulStop(actor, 30 seconds).map(_ => Done)
    }
  }

  CoordinatedShutdown(system).addTask(CoordinatedShutdown.PhaseServiceStop, "stopControlServer") { () =>
    log.info("Shutting down control server...")

    for{
      _ <- Kamon.stopAllReporters()
      _ <- IO(Http).ask(Http.CloseAll)
    } yield Done
  }

  def shutdown() = {
    CoordinatedShutdown(system).run(CoordinatedShutdown.JvmExitReason)
  }

  def shutdownAndExit(code: Int) = shutdown().map(_ => sys.exit(code))

  def inFlightIncrement(count: Long) = {
    inFlightMetric.increment(count)
    inFlightCount.addAndGet(count)
  }

  def inFlightDecrement(count: Long) = {
    inFlightMetric.decrement(count)
    inFlightCount.addAndGet(-1 * count)
  }

  def inFlightReset = {
    inFlightCount.set(0)
  }

  /** Allows the configuration for the listener object to be set on startup.
    * The listener will look for whitelist, blacklist, and emitter settings.
    *
    * @param config
    */
  def setConfig(config: Config) = {
    whitelist.clear()
    blacklist.clear()

    if(config.hasPath("whitelist")) {
      config.getString("whitelist").split(',').foreach {
        case table =>
          log.info("Adding table to whitelist: {}", table)
          whitelist.add(table)
      }
    }
    else if(config.hasPath("blacklist")) {
      config.getString("blacklist").split(',').foreach {
        case table =>
          log.info("Adding table to blacklist: {}", table)
          blacklist.add(table)
      }
    }

    if(config.hasPath("position-saver.actor")) {
      createActor(config.getString("position-saver.actor"), "positionSaverActor", config).
        foreach(actorRef => setPositionSaver(actorRef))
    }
    else {
      positionSaver = positionSaver match {
        case None => Some(system.actorOf(Props(new PositionSaver()), name = "positionSaverActor"))
        case _ => positionSaver
      }
    }

    if(config.hasPath("emitter")) {
      val positionSaverLoader: ActorRefFactory => ActorRef = _ => positionSaver.get
      createActor(config.getString("emitter"), "emitterActor", positionSaverLoader, config).
        foreach(actorRef => setEmitter(actorRef))
    }
    else {
      emitter = emitter match {
        case None => Some(system.actorOf(Props(new StdoutActor(_ => positionSaver.get)), name = "emitterActor"))
        case _ => emitter
      }
    }

    if(config.hasPath("in-flight-limit")) {
      inFlightLimit = config.getLong("in-flight-limit") match {
        case limit if limit > 0 =>
          Some(limit)
        case _ => None
      }
    }

    if(config.hasPath("in-flight-limit-sleep-time")) {
      val sleepTime = config.getLong("in-flight-limit-sleep-time")
      if(sleepTime > 0) {
        inFlightLimitSleepTime = sleepTime
      }
    }
  }

  /** Start the HTTP server for status and control **/
  def startControlServer(config: Config) = {
    val controlBind = Http.Bind(
      listener = system.actorOf(Props[ControlInterfaceActor], "control-interface"),
      interface = config.getString("control.host"),
      port = config.getInt("control.port")
    )

    IO(Http).ask(controlBind).map {
      case Http.Bound(address) =>
        log.info("Control interface bound to {}", address)
      case Http.CommandFailed(cmd) =>
        log.warn("Could not bing control interface: {}", cmd.failureMessage)
    }

    Kamon.loadReportersFromConfig()
  }

  /** Allows the position saver actor to be configured at runtime.
    *
    * Note: you must initialize the position saver before the emitter, or it will be ignored
    *
    * @param actor your actor ref
    */
  def setPositionSaver(actor: ActorRef) = positionSaver = Some(actor)

  /** Allows the emitter/producer actor to be configured at runtime.
    *
    * Note: you must initialize the emitter before calling setConfig, or it will be ignored
    *
    * @param actor your actor ref
    */
  def setEmitter(actor: ActorRef) = emitter = Some(actor)

  def getStoredPosition = askPositionSaver(GetLastSavedPositionRequest)
  def setPosition(position: Option[String]) = askPositionSaver(SavePositionRequest(position))
  def persistPosition = askPositionSaver(SaveCurrentPositionRequest)

  protected def askPositionSaver(message: Any): Future[Option[String]] = {
    import akka.pattern.ask
    implicit val ec = system.dispatcher
    implicit val timeout = Timeout(60.seconds)

    positionSaver match {
      case Some(saver) =>
        saver.ask(message) map {
          case GetPositionResponse(maybePosition: Option[String]) =>
            maybePosition
        }
      case None =>
        log.error("Position saver actor is not initialized!!!!!!!!")
        throw new Exception("Position saver actor is not initialized!!!!!!!!")
    }
  }

  /** Sends binlog events to the appropriate changestream actor.
    *
    * @param binaryLogEvent The binlog event
    */
  def onEvent(binaryLogEvent: Event) = {
    val header = binaryLogEvent.getHeader[EventHeaderV4]
    counterMetric.refine("event_type" -> header.getEventType.toString).increment()

    log.debug("Received event: {}", binaryLogEvent)
    val changeEvent = getChangeEvent(binaryLogEvent, header)

    changeEvent match {
      case Some(e: TransactionEvent)  => transactionActor ! e
      case Some(e: MutationEvent)     =>
        ChangeStreamEventListener.inFlightIncrement(e.rows.length)
        val nextPosition = getNextPosition
        transactionActor ! MutationWithInfo(e, nextPosition)
      case Some(e: AlterTableEvent)   => columnInfoActor ! e
      case None =>
        log.debug("Ignoring {} event.", binaryLogEvent.getHeader[EventHeaderV4].getEventType)
    }

    blockIfAboveInFlightLimit
  }

  def getNextPosition: String = {
    // TODO make position a case class so we can use position.copy(position = 123) notation
    val safePosition = currentTransactionPosition.getOrElse(
      currentRowsQueryPosition.getOrElse(
        currentTableMapPosition.get
      )
    )
    s"${currentBinlogFile.get}:${safePosition.toString}"
  }

  def getCurrentPosition = {
    val safePosition = currentTransactionPosition.getOrElse(
      currentRowsQueryPosition.getOrElse(
        currentTableMapPosition.getOrElse(0L)
      )
    )
    s"${currentBinlogFile.getOrElse("<not started>")}:${safePosition.toString}"
  }

  /** Returns the appropriate ChangeEvent case object given a binlog event object.
    *
    * @param event The java binlog listener event
    * @return The resulting ChangeEvent
    */
  def getChangeEvent(event: Event, header: EventHeaderV4): Option[ChangeEvent] = {
    log.debug("Got event of type {} with {}", header.getEventType, header.getNextPosition)

    header.getEventType match {
      case eventType if EventType.isRowMutation(eventType) =>
        getMutationEvent(event, header)

      case GTID =>
        Some(Gtid(event.getData[GtidEventData].getGtid))

      case XID =>
        currentTransactionPosition = None
        currentRowsQueryPosition = None
        currentTableMapPosition = Some(header.getNextPosition) // use this as a last resort behind the other three
        Some(CommitTransaction(header.getNextPosition))

      case QUERY =>
        parseQueryEvent(event.getData[QueryEventData], header) match {
          case Some(BeginTransaction) =>
            currentTransactionPosition = Some(header.getPosition)
            Some(BeginTransaction)
          case x => x
        }

      case FORMAT_DESCRIPTION =>
        val data = event.getData[FormatDescriptionEventData]
        log.info("Server version: {}, binlog version: {}", data.getServerVersion, data.getBinlogVersion)
        None

      case ROTATE =>
        val rotateEvent = event.getData[RotateEventData]
        currentBinlogFile = Some(rotateEvent.getBinlogFilename)
        currentTransactionPosition = None
        currentRowsQueryPosition = None
        currentTableMapPosition = Some(header.getPosition) // use this as a last resort behind the other three
        None

      case TABLE_MAP =>
        currentTableMapPosition = Some(header.getPosition)
        None

      case ROWS_QUERY =>
        currentRowsQueryPosition = Some(header.getPosition)
        None

      // Known events that are safe to ignore
      case PREVIOUS_GTIDS => None
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

        maybeIdentifier.flatMap {
          case (db, table) if !tableInBlacklist(db.toLowerCase, table.toLowerCase) =>
            Some(AlterTableEvent(db.toLowerCase, table.toLowerCase, sql))
          case _ => None
        }
      case sql =>
        None
    }
  }

  protected def unescapeIdentifier(escaped: String) = escaped.charAt(0) match {
    case '`' => escaped.substring(1, escaped.length - 1).replace("``", "`")
    case '"' => escaped.substring(1, escaped.length - 1).replace("\"\"", "\"")
    case _ => escaped
  }

  private def blockIfAboveInFlightLimit = {
    inFlightLimit.foreach {
      case limit:Long =>
        val currentInFlight = inFlightCount.get()
        if(currentInFlight > limit) {
          log.debug(s"Hit in flight limit (${currentInFlight} in flight / ${limit} limit).")
          Thread.sleep(inFlightLimitSleepTime)
        }
    }
  }

  private def createActor(classString: String, actorName: String, args: Object*):Option[ActorRef] = {
    try {
      val constructor = Class.forName(classString).asInstanceOf[Class[Actor]].getDeclaredConstructors.head
      lazy val actorInstance = constructor.newInstance(args: _*).asInstanceOf[Actor]
      Some(system.actorOf(Props(actorInstance), name = actorName))
    }
    catch {
      case e: ClassNotFoundException =>
        log.error(s"Couldn't load emitter class ${classString} (ClassNotFoundException), using the default emitter.", e)
        None
    }
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
