package changestream.actors

import java.io._
import java.nio.charset.StandardCharsets

import scala.concurrent.duration._
import scala.language.postfixOps
import akka.actor.{Actor, ActorRef, Cancellable}
import changestream.ChangeStreamEventListener
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory

import scala.io.Source

object PositionSaver {
  // TODO: position should be a struct
  // TODO: gtid support
  case class SavePositionRequest(positionOverride: Option[String])
  case object SaveCurrentPositionRequest
  case object GetPositionRequest
  case object GetLastSavedPositionRequest
  case object RestoreLastSavedPositionRequest
  case class GetPositionResponse(position: Option[String])
  case class EmitterResult(position: String, eventCount: Long = 1, meta: Option[Any] = None)
}

class PositionSaver(config: Config = ConfigFactory.load().getConfig("changestream")) extends Actor {
  import PositionSaver._

  protected val log = LoggerFactory.getLogger(getClass)
  protected implicit val ec = context.dispatcher

  protected val MAX_RECORDS = config.getLong("position-saver.max-records")
  protected val MAX_WAIT = config.getLong("position-saver.max-wait").milliseconds

  protected val SAVER_FILE_PATH = config.getString("position-saver.file-path")
  protected lazy val saverFile = new File(SAVER_FILE_PATH)

  protected def setDelayedSave(origSender: ActorRef) = {
    val scheduler = context.system.scheduler
    cancelDelayedSave
    cancellableSchedule = MAX_WAIT.length match {
      case 0 => None
      case _ => Some(scheduler.scheduleOnce(MAX_WAIT) {
        log.debug("Initiating delayed save.")
        self.tell(SaveCurrentPositionRequest, origSender)
      })
    }
  }
  protected def cancelDelayedSave = cancellableSchedule.foreach(_.cancel())

  // Mutable State!!
  protected var cancellableSchedule: Option[Cancellable] = None
  protected var currentRecordCount = 0
  protected var currentPosition: Option[String] = None
  // End Mutable State!!

  private def readPosition: Option[String] = {
    if(saverFile.exists()) {
      try {
        val bufferedSource = Source.fromFile(saverFile, "UTF-8")
        val position = bufferedSource.getLines.mkString match {
          case "" => None
          case str:String => Some(str)
        }
        bufferedSource.close
        position
      } catch {
        case exception: IOException =>
          log.error(s"Failed to read position from position file (${SAVER_FILE_PATH}).", exception)
          throw exception
      }
    }
    else {
      None
    }
  }

  def writePosition(position: Option[String], sender: Option[ActorRef] = None) = {
    try {
      cancelDelayedSave
      currentRecordCount = 0

      val saverOutputStream = new FileOutputStream(saverFile)
      val saverWriter = new OutputStreamWriter(saverOutputStream, StandardCharsets.UTF_8)
      saverWriter.write(position match {
        case None => "" //TODO none and empty string mean the same thing right now. Is this cool?
        case Some(str) => str
      })
      saverWriter.close()
      log.trace("Save success at {}", position)

      sender.map(_ ! akka.actor.Status.Success(GetPositionResponse(position)))
    } catch {
      case exception: IOException =>
        log.error(s"Failed to write position to position file (${SAVER_FILE_PATH}).", exception)
        sender.map(_ ! akka.actor.Status.Failure(exception))
        throw exception
    }
  }

  def restoreLastSavedPosition = currentPosition = readPosition

  override def preStart() = {
    restoreLastSavedPosition
    writePosition(currentPosition)
    log.info(s"Ready to save positions to file ${SAVER_FILE_PATH} (max-records=${MAX_RECORDS}, max-wait=${MAX_WAIT}).")
  }

  override def postStop() = {
    log.info("Shutting down PositionSaver and saving current position: {}", currentPosition)
    writePosition(currentPosition, None)
  }

  def receive = {
    case EmitterResult(position, eventCount, meta) =>
      // TODO: this will steadily increase with each permanent failure to emit (i.e. failed to publish to s3). This is probably fine
      ChangeStreamEventListener.inFlightDecrement(eventCount)

      currentRecordCount += 1
      currentPosition = Some(position)

      currentRecordCount match {
        case MAX_RECORDS =>
          log.debug("Received position: {} (persisting immediately)", position)
          writePosition(currentPosition, Some(sender()))
        case _ =>
          log.debug("Received position: {} (setting delayed save)", position)
          setDelayedSave(sender())
      }

    case SavePositionRequest(overridePosition: Option[String]) =>
      log.info(s"Saving override position: ${overridePosition} (old position: ${currentPosition})")
      currentPosition = overridePosition
      writePosition(currentPosition, Some(sender()))

    case SaveCurrentPositionRequest =>
      log.debug("Saving current position: {}", currentPosition)
      writePosition(currentPosition, Some(sender()))

    case GetPositionRequest =>
      log.debug("Received request for current position: {}", currentPosition)
      sender() ! GetPositionResponse(currentPosition)

    case GetLastSavedPositionRequest =>
      val lastSavedPosition = readPosition
      log.debug(s"Received request for last saved position: ${lastSavedPosition} (current: ${currentPosition}")
      sender() ! GetPositionResponse(lastSavedPosition)

    case RestoreLastSavedPositionRequest =>
      val previousPosition = currentPosition
      restoreLastSavedPosition
      log.info(s"Restoring last saved position: ${currentPosition} (previously: ${previousPosition}")
      sender() ! GetPositionResponse(currentPosition)
  }
}
