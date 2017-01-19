package changestream.actors

import akka.actor._
import akka.persistence._
import akka.util.Timeout

import scala.concurrent.duration._
import scala.language.postfixOps
import changestream.actors.BinlogClient.{Connect, Disconnect}
import changestream.actors.EmitterCoordinator.{ResetPosition, StartChangeStream}
import changestream.events.{BinlogPosition, MutationWithInfo}
import org.slf4j.LoggerFactory

object EmitterCoordinator {
  case object StartChangeStream
  case object ResetPosition
}

//TODO delete performance
//TODO spec and handle failure cases (emitter failure, snapshot save failure, inbox growing too fast failure)
class EmitterCoordinator(
                          getBinlogClientActor: ActorRefFactory => ActorRef,
                          overridePersistenceId: String = "changestream-emmiter-coordinator"
                        ) extends PersistentActor {
  override def persistenceId = overridePersistenceId
  override def journalPluginId = "inmem-journal"

  protected val binlogClient = getBinlogClientActor(context)
  protected val log = LoggerFactory.getLogger(getClass)

  protected implicit val timeout = Timeout(10 seconds) // TODO
  protected implicit val ec = context.dispatcher

  var state:Option[BinlogPosition] = None

  def afterPersist(successfulMutation: MutationWithInfo) = updateState(successfulMutation.position)

  protected def connect() = binlogClient ! Connect(state)
  protected def disconnect() = binlogClient ! Disconnect
  protected def reset() = binlogClient ! ResetPosition

  def updateState(newPosition: Option[BinlogPosition]) = {
    log.debug(s"Updating last successful position to ${newPosition}")
    state = newPosition

    saveSnapshot(state)
  }

  val receiveRecover: Receive = {
    case SnapshotOffer(_, snapshot: Option[BinlogPosition]) =>
      log.debug(s"Recovering saved position ${snapshot}")
      state = snapshot

    case RecoveryCompleted =>
      log.info(s"Recovered position at ${state}, starting binlog client.")
      connect()
  }

  val receiveCommand: Receive = {
    case mutation: MutationWithInfo =>
      log.debug(s"Received mutation: ${mutation}")
      persistAsync(mutation)(afterPersist)

    case SaveSnapshotSuccess(meta) =>
      log.debug(s"Successfully saved last position.")
      deleteMessages(meta.sequenceNr)
      deleteSnapshots(SnapshotSelectionCriteria.create(meta.sequenceNr-1, meta.timestamp-1))

    case StartChangeStream =>
      log.info(s"Received start signal. Starting binlog at position ${state}")
      connect()

    case ResetPosition =>
      log.debug("Resetting position")
      updateState(None)

    case SaveSnapshotFailure(_, reason) =>
      log.error(s"Stopping the binlog consumer because we couldn't save last position: ${reason}.")
      disconnect()
  }

  override def onPersistFailure(cause: Throwable, event: Any, seqNr: Long) = {
    log.error(s"Failed to emit message: ${cause}.")
    disconnect()
    super.onPersistFailure(cause, event, seqNr)
  }

  override def onRecoveryFailure(cause: Throwable, event: Option[Any]) = {
    log.error(s"Failed to recover position from snapshot: ${cause}.")
    disconnect()
    super.onRecoveryFailure(cause, event)
  }

  override def onPersistRejected(cause: Throwable, event: Any, seqNr: Long) = {
    log.error(s"Message was rejected by emitter plugin: ${cause}.")
    disconnect()
    super.onPersistRejected(cause, event, seqNr)
  }
}
