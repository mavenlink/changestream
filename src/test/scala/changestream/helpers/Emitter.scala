package changestream.helpers

import java.io.File

class Emitter extends Base with Config {
  val (messageNoJson, _, _) = Fixtures.mutationWithInfo("insert", 1, 1, false, true, 1)
  val message = messageNoJson.copy(formattedMessage = Some("{json:true}"))
  val INVALID_MESSAGE = 0
  val snapshotDir = testConfig.getString("akka.persistence.snapshot-store.local.dir")

  after {
    new File(snapshotDir).listFiles().foreach(_.delete())
  }
}
