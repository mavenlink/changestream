package changestream.persistence

import akka.persistence.journal.JournalSpec
import com.typesafe.config.ConfigFactory

import scala.concurrent.Future

class EmitterBaseSpec extends JournalSpec(
  config = ConfigFactory.parseString(
    """
    akka.persistence.journal.plugin = "base-emitter-tester"
    base-emitter-tester {
      class = "changestream.persistence.TesterPlugin"
      plugin-dispatcher = "akka.actor.default-dispatcher"
    }
    """)) {

  override def supportsRejectingNonSerializableObjects = false
  override def supportsAtomicPersistAllOfSeveralEvents = false
}

class TesterPlugin extends EmitterBase {
  override def asyncEmitPayload(persistenceId: String, payload: Any) = {
    Future.successful(())
  }
}
