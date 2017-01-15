package changestream.actors

import akka.actor._
import akka.event.LoggingReceive
import akka.persistence._
import akka.util.Timeout

import scala.concurrent.duration._
import scala.language.postfixOps
import changestream.actors.BinlogClient.Connect
import changestream.actors.EmitterCoordinator.{ResetPosition, StartChangeStream}
import changestream.events.{BinlogPosition, MutationWithInfo}
import org.slf4j.LoggerFactory

object EmitterCoordinator {
  case object StartChangeStream
  case object ResetPosition
}

//TODO add snapshot stuff
//TODO does snapshot clear the journal?
class EmitterCoordinator(
                          getBinlogClient: ActorRefFactory => ActorRef,
                          overridePersistenceId: String = "changestream-emmiter-coordinator"
                        ) extends PersistentActor {
  override def persistenceId = overridePersistenceId
  override def journalPluginId = "inmem-journal"

  protected val binlogClient = getBinlogClient(context)
  protected val log = LoggerFactory.getLogger(getClass)

  protected implicit val timeout = Timeout(10 seconds) // TODO
  protected implicit val ec = context.dispatcher

  var state:Option[BinlogPosition] = None

  def afterPersist(successfulMutation: MutationWithInfo) = {
    updateState(successfulMutation.position)
  }

  def updateState(newPosition: Option[BinlogPosition]) = {
    log.debug(s"Updating last successful position to ${newPosition}")
    state = newPosition
    saveSnapshot(state)
  }

  val receiveRecover: Receive = {
    case SnapshotOffer(_, snapshot: Option[BinlogPosition]) =>
      log.debug(s"Recovering saved position ${snapshot}")
      state = snapshot
  }

  val receiveCommand: Receive = LoggingReceive({
    case StartChangeStream =>
      log.info(s"Received start signal. Starting binlog at position ${state}")
      binlogClient ! Connect(state)

    case ResetPosition =>
      log.debug("Resetting position")
      updateState(None)

    case mutation: MutationWithInfo =>
      log.debug(s"Received mutation: ${mutation}")
      persistAsync(mutation)(afterPersist)

    case SaveSnapshotSuccess(meta) =>
      log.debug("Successfully saved last position.")
      deleteMessages(meta.sequenceNr)
      deleteSnapshots(SnapshotSelectionCriteria.create(meta.sequenceNr-1, meta.timestamp-1))

    case SaveSnapshotFailure(_, reason) =>
      log.error(s"Couldn't save last position ${reason}.")
  })
}
