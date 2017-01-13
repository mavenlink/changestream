package changestream.actors

import akka.actor.ActorRef
import akka.event.LoggingReceive
import akka.persistence._
import changestream.events.{BinlogPosition, MutationWithInfo}

object EmitterJournalTester {
  case class PersistRejected(cause: Throwable, event: Any, seqNr: Long)
  case class PersistFailed(cause: Throwable, event: Any, seqNr: Long)
}

class EmitterJournalTester(journalPlugin: String, probe: ActorRef, topic: String = "journal-test") extends PersistentActor {
  import EmitterJournalTester._

  override def persistenceId = topic
  override def journalPluginId = journalPlugin

  var state:Option[BinlogPosition] = None

  def afterPersist(payload: Any) = payload match {
    case successfulMutation: MutationWithInfo =>
      state = successfulMutation.position
      probe ! state
  }

  override def onPersistFailure(cause: Throwable, event: Any, seqNr: Long) = {
    probe ! PersistFailed(cause, event, seqNr)
  }

  override def onPersistRejected(cause: Throwable, event: Any, seqNr: Long) = {
    probe ! PersistRejected(cause, event, seqNr)
  }

  val receiveRecover: Receive = LoggingReceive({
    case mutation: MutationWithInfo => ()
  })

  val receiveCommand: Receive = LoggingReceive({
    case x:Any =>
      persistAsync(x)(afterPersist)
  })
}
