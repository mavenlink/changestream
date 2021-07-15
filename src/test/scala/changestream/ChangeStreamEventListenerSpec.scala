package changestream

import java.util

import akka.actor.ActorRefFactory
import akka.testkit.TestProbe
import changestream.events._
import changestream.helpers.{Base, Config}
import com.github.shyiko.mysql.binlog.event.EventType._
import com.github.shyiko.mysql.binlog.event._
import com.typesafe.config.ConfigFactory

import scala.reflect.ClassTag

class ChangeStreamEventListenerSpec extends Base with Config {
  def getTypedEvent[T: ClassTag](event: Event): Option[T] = ChangeStreamEventListener.getChangeEvent(event, event.getHeader[EventHeaderV4]) match {
    case Some(e: T) => Some(e)
    case _ => None
  }

  val header = new EventHeaderV4()

  val probe = TestProbe()
  ChangeStreamEventListener.setEmitter(probe.ref)

  "ChangeStreamEventListener" should {
    "Should not crash when receiving a ROTATE event" in {
      header.setEventType(EventType.ROTATE)
      val rotate = new Event(header, new RotateEventData())

      ChangeStreamEventListener.onEvent(rotate)
    }
    "Should not crash when receiving a STOP event" in {
      header.setEventType(EventType.STOP)
      val stop = new Event(header, null)

      ChangeStreamEventListener.onEvent(stop)
    }
    "Should not crash when receiving a FORMAT_DESCRIPTION event" in {
      header.setEventType(EventType.FORMAT_DESCRIPTION)
      val rotate = new Event(header, new FormatDescriptionEventData())

      ChangeStreamEventListener.onEvent(rotate)
    }
  }

  "When a custom emitter is specified in the config" should {
    "use that emitter" in {
      val emitterConfig = ConfigFactory
        .parseString("changestream.emitter = \"changestream.actors.StdoutActor\"")
        .withFallback(testConfig)
        .getConfig("changestream")

      ChangeStreamEventListener.setConfig(emitterConfig)
    }
  }

  "When keeping track of safe positions" should {
    "treat begin transaction as safe when begin transaction is present" in {
      header.setEventType(ROTATE)
      header.setNextPosition(1)
      val rotateData = new RotateEventData()
      rotateData.setBinlogFilename("foo")
      rotateData.setBinlogPosition(1)
      val rotate = new Event(header, rotateData)
      ChangeStreamEventListener.onEvent(rotate)

      header.setEventType(QUERY)
      header.setNextPosition(3)
      val queryData = new QueryEventData()
      queryData.setSql("begin")
      val query = new Event(header, queryData)
      ChangeStreamEventListener.onEvent(query)

      header.setEventType(ROWS_QUERY)
      header.setNextPosition(4)
      val data = new IntVarEventData()
      val rows_query = new Event(header, data)
      ChangeStreamEventListener.onEvent(rows_query)

      header.setEventType(TABLE_MAP)
      header.setNextPosition(5)
      val table_map = new Event(header, data)
      ChangeStreamEventListener.onEvent(table_map)

      ChangeStreamEventListener.getNextPosition should be("foo:3")
    }

    "treat rows query as safe when rows query is present" in {
      header.setEventType(ROTATE)
      header.setNextPosition(1)
      val rotateData = new RotateEventData()
      rotateData.setBinlogFilename("foo")
      rotateData.setBinlogPosition(1)
      val rotate = new Event(header, rotateData)
      ChangeStreamEventListener.onEvent(rotate)

      header.setEventType(ROWS_QUERY)
      header.setNextPosition(2)
      val data = new IntVarEventData()
      val rows_query = new Event(header, data)
      ChangeStreamEventListener.onEvent(rows_query)

      header.setEventType(TABLE_MAP)
      header.setNextPosition(3)
      val table_map = new Event(header, data)
      ChangeStreamEventListener.onEvent(table_map)

      ChangeStreamEventListener.getNextPosition should be("foo:2")
    }

    "treat table map as safe when rows query is not present" in {
      header.setEventType(ROTATE)
      header.setNextPosition(1)
      val rotateData = new RotateEventData()
      rotateData.setBinlogFilename("foo")
      rotateData.setBinlogPosition(1)
      val rotate = new Event(header, rotateData)
      ChangeStreamEventListener.onEvent(rotate)

      header.setEventType(TABLE_MAP)
      header.setNextPosition(3)
      val data = new IntVarEventData()
      val table_map = new Event(header, data)
      ChangeStreamEventListener.onEvent(table_map)

      ChangeStreamEventListener.getNextPosition should be("foo:3")
    }

    "rows query and table map positions are replaced with XID nextPosition after XID event is received" in {
      header.setEventType(ROTATE)
      header.setNextPosition(1)
      val rotateData = new RotateEventData()
      rotateData.setBinlogFilename("foo")
      rotateData.setBinlogPosition(1)
      val rotate = new Event(header, rotateData)
      ChangeStreamEventListener.onEvent(rotate)

      header.setEventType(ROWS_QUERY)
      header.setNextPosition(2)
      val data = new IntVarEventData()
      val rows_query = new Event(header, data)
      ChangeStreamEventListener.onEvent(rows_query)

      header.setEventType(TABLE_MAP)
      header.setNextPosition(3)
      val table_map = new Event(header, data)
      ChangeStreamEventListener.onEvent(table_map)

      header.setEventType(XID)
      header.setNextPosition(4)
      val xid = new Event(header, data)
      ChangeStreamEventListener.onEvent(xid)

      ChangeStreamEventListener.getNextPosition should be("foo:4")
    }
  }

  "When receiving an invalid event" should {
    "Throw an exception" in {
      header.setEventType(CREATE_FILE)
      val data = new IntVarEventData()
      val event = new Event(header, data)

      assertThrows[java.lang.Exception] {
        ChangeStreamEventListener.onEvent(event)
      }
    }
  }

  "When receiving a mutation event" should {
    "Properly increment the sequence number" in {
      header.setEventType(WRITE_ROWS)

      val tableId = 123
      val data = Insert(tableId, new util.BitSet(3), List(Array(1, "peter", "password")))
      val event1 = new Event(header, data.copy(sequence = 0))
      val event2 = new Event(header, data.copy(sequence = 1))

      val seq1 = getTypedEvent[MutationEvent](event1).get.sequence
      val seq2 = getTypedEvent[MutationEvent](event2).get.sequence

      seq1 should be(seq2-1)
    }

    "Properly increment the sequence number when there are many rows in the mutation" in {
      header.setEventType(WRITE_ROWS)

      val tableId = 123
      val dataMany = Insert(tableId, new util.BitSet(3), List(Array(1, "peter", "password"), Array(2, "anna", "password")))
      val eventMany = new Event(header, dataMany.copy(sequence = 0))

      val dataSingle = Insert(tableId, new util.BitSet(3), List(Array(1, "peter", "password")))
      val eventOne = new Event(header, dataSingle.copy(sequence = 2)) // BAD SPEC.. not testing anything

      val seqMany = getTypedEvent[MutationEvent](eventMany).get.sequence
      val seqOne = getTypedEvent[MutationEvent](eventOne).get.sequence

      seqOne should be(seqMany + 2)
    }
  }

  "When receiving an insert mutation event" should {
    "Emit a MutationEvent(Insert(...)..)" in {
      header.setEventType(WRITE_ROWS)

      val tableId = 123
      val data = Insert(tableId, new util.BitSet(3), List[Array[java.io.Serializable]]())
      val event1 = new Event(header, data)

      getTypedEvent[Insert](event1) should be(Some(data))

      header.setEventType(EXT_WRITE_ROWS)
      val event2 = new Event(header, data)

      getTypedEvent[Insert](event2) should be(Some(data))
    }
  }

  "When receiving an update mutation event" should {
    "Emit a MutationEvent(Update(...)..)" in {
      header.setEventType(UPDATE_ROWS)

      val tableId = 123
      val data = Update(tableId, new util.BitSet(3), new util.BitSet(3), List[Array[java.io.Serializable]](), List[Array[java.io.Serializable]]())
      val event1 = new Event(header, data)

      getTypedEvent[Update](event1) should be(Some(data))

      header.setEventType(EXT_UPDATE_ROWS)
      val event2 = new Event(header, data)

      getTypedEvent[Update](event2) should be(Some(data))
    }
  }

  "When receiving an delete mutation event" should {
    "Emit a MutationEvent(Delete(...)..)" in {
      header.setEventType(DELETE_ROWS)

      val tableId = 123
      val data = Delete(tableId, new util.BitSet(3), List[Array[java.io.Serializable]]())
      val event1 = new Event(header, data)

      getTypedEvent[Delete](event1) should be(Some(data))

      header.setEventType(EXT_DELETE_ROWS)
      val event2 = new Event(header, data)

      getTypedEvent[Delete](event2) should be(Some(data))
    }
  }

  "When a white/black list is enabled" should {
    "include tables on the whitelist" in {
      val whitelistConfig = ConfigFactory
        .parseString("changestream.whitelist = \"changestream_test.users\"")
        .withFallback(testConfig)
        .getConfig("changestream")

      ChangeStreamEventListener.setConfig(whitelistConfig)

      header.setEventType(WRITE_ROWS)

      val tableId = 123
      val data = Insert(tableId, new util.BitSet(3), List[Array[java.io.Serializable]](),
        database = "changestream_test", tableName = "users")
      val event = new Event(header, data)

      getTypedEvent[Insert](event) should be(Some(data))
    }

    "and exclude those on the blacklist" in {
      val blacklistConfig = ConfigFactory
        .parseString("changestream.blacklist = \"changestream_test.users,blah.not_important\"")
        .withFallback(testConfig)
        .getConfig("changestream")

      ChangeStreamEventListener.setConfig(blacklistConfig)

      header.setEventType(WRITE_ROWS)

      val tableId = 123
      val data = Insert(tableId, new util.BitSet(3), List[Array[java.io.Serializable]](),
        database = "changestream_test", tableName = "users")
      val event = new Event(header, data)

      getTypedEvent[Insert](event) should be(None)
    }

    "and exclude special databases" in {
      Seq("information_schema", "mysql", "performance_schema", "sys").foreach({ invalidDb =>
        header.setEventType(WRITE_ROWS)
        val tableId = 123
        val data = Insert(tableId, new util.BitSet(3), List[Array[java.io.Serializable]](),
          database = invalidDb, tableName = "any_table")
        val event = new Event(header, data)

        getTypedEvent[Insert](event) should be(None)
      })
    }
  }

  "When receiving a XID event" should {
    "Emit a TransactionEvent(CommitTransaction..)" in {
      header.setEventType(XID)
      header.setNextPosition(42)
      val data = new XidEventData()
      val event = new Event(header, data)

      ChangeStreamEventListener.onEvent(event)

      getTypedEvent[TransactionEvent](event) should be(Some(CommitTransaction(42)))
    }
  }

  "When receiving a QUERY event for Transaction" should {
    "Emit a TransactionEvent(BeginTransaction..) for BEGIN query" in {
      header.setEventType(QUERY)
      val data = new QueryEventData()
      data.setSql("BEGIN")
      val event = new Event(header, data)

      getTypedEvent[TransactionEvent](event) should be(Some(BeginTransaction))
    }
    "Emit a TransactionEvent(CommitTransaction..) for COMMIT query" in {
      header.setEventType(QUERY)
      header.setNextPosition(42)
      val data = new QueryEventData()
      data.setSql("COMMIT")
      val event = new Event(header, data)

      getTypedEvent[TransactionEvent](event) should be(Some(CommitTransaction(42)))
    }
    "Emit a TransactionEvent(RollbackTransaction..) for ROLLBACK query" in {
      header.setEventType(QUERY)
      val data = new QueryEventData()
      data.setSql("ROLLBACK")
      val event = new Event(header, data)

      getTypedEvent[TransactionEvent](event) should be(Some(RollbackTransaction))
    }
  }

  "When receiving a QUERY event for ALTER and emitting AlterTableEvent" should {
    header.setEventType(QUERY)
    val data = new QueryEventData()

    "Emit correct event for no-ignore" in {
      data.setDatabase("changestream_test")
      data.setSql("ALTER TABLE tbl_name")
      val alter = getTypedEvent[AlterTableEvent](new Event(header, data)).get
      alter.database should be("changestream_test")
      alter.tableName should be("tbl_name")
    }

    "Emit correct event for ignore" in {
      data.setDatabase("changestream_test")
      data.setSql("ALTER IGNORE TABLE tbl_name")
      val alter = getTypedEvent[AlterTableEvent](new Event(header, data)).get
      alter.database should be("changestream_test")
      alter.tableName should be("tbl_name")
    }

    "Emit correct event for escaped table name" in {
      data.setDatabase("changestream_test")
      data.setSql("ALTER TABLE `tbl_name`")
      val alter = getTypedEvent[AlterTableEvent](new Event(header, data)).get
      alter.database should be("changestream_test")
      alter.tableName should be("tbl_name")
    }

    "Emit correct event for quote escaped table name" in {
      data.setDatabase("changestream_test")
      data.setSql("ALTER TABLE \"tbl_name\"")
      val alter = getTypedEvent[AlterTableEvent](new Event(header, data)).get
      alter.database should be("changestream_test")
      alter.tableName should be("tbl_name")
    }

    "Emit correct event for escaped table name and database" in {
      data.setSql("ALTER TABLE `database`.`tbl_name`")
      val alter = getTypedEvent[AlterTableEvent](new Event(header, data)).get
      alter.database should be("database")
      alter.tableName should be("tbl_name")
    }

    "Emit correct event for quote escaped table name and database" in {
      data.setSql("ALTER TABLE \"database\".\"tbl_name\"")
      val alter = getTypedEvent[AlterTableEvent](new Event(header, data)).get
      alter.database should be("database")
      alter.tableName should be("tbl_name")
    }

    "Emit correct event for escaped table name and non-escaped database" in {
      data.setSql("ALTER TABLE database.`tbl_name`")
      val alter = getTypedEvent[AlterTableEvent](new Event(header, data)).get
      alter.database should be("database")
      alter.tableName should be("tbl_name")
    }

    "Emit correct event for quote escaped table name and non-escaped database" in {
      data.setSql("ALTER TABLE database.\"tbl_name\"")
      val alter = getTypedEvent[AlterTableEvent](new Event(header, data)).get
      alter.database should be("database")
      alter.tableName should be("tbl_name")
    }

    "Exclude those on the blacklist" in {
      val blacklistConfig = ConfigFactory
        .parseString("changestream.blacklist = \"database.blocked_table_name\"")
        .withFallback(testConfig)
        .getConfig("changestream")

      ChangeStreamEventListener.setConfig(blacklistConfig)

      data.setSql("ALTER TABLE database.\"blocked_table_name\"")
      getTypedEvent[AlterTableEvent](new Event(header, data)) should be(None)
    }
  }
}
