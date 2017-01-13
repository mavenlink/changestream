package changestream.persistence

import akka.persistence.journal.JournalSpec
import com.typesafe.config.ConfigFactory

class InmemJournalSpec extends JournalSpec(
  config = ConfigFactory.parseString(
    """
    akka.persistence.journal.plugin = "in-mem-journal"
    in-mem-journal {
      class = "changestream.persistence.InmemJournal"
      plugin-dispatcher = "akka.actor.default-dispatcher"
    }
    """)) {

  override def supportsRejectingNonSerializableObjects = false
}
