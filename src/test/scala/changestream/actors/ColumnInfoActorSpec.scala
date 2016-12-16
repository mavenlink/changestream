package changestream.actors

import akka.testkit.{TestActorRef, TestProbe}
import akka.actor.{ActorRefFactory, Props}

import scala.concurrent.duration._
import scala.language.postfixOps
import changestream.events._
import changestream.helpers.{Database, Fixtures}
import com.typesafe.config.ConfigFactory

class ColumnInfoActorSpec extends Database {
  val probe = TestProbe()
  val maker = (_: ActorRefFactory) => probe.ref
  val columnInfoActor = TestActorRef(Props(classOf[ColumnInfoActor], maker, config))

  val probeNoPreload = TestProbe()
  val makerNoPreload = (_: ActorRefFactory) => probeNoPreload.ref
  val configNoPreload = ConfigFactory.load().getConfig("changestream.mysql")
  val columnInfoActorNoPreload = TestActorRef(Props(classOf[ColumnInfoActor], makerNoPreload, configNoPreload))

  val database = "changestream_test"
  val tableName = "users"
  val (fakeMutationWithNoInfo, _, _) =
    Fixtures.mutationWithInfo("insert", transactionInfo = false, columns = false, database = database, tableName = "fake_table")
  val (mutationWithInfo, _, _) = Fixtures.mutationWithInfo("insert", transactionInfo = false, columns = true, database = database, tableName = tableName)
  val mutationWithNoInfo = mutationWithInfo.copy(columns = None)
  val columns = mutationWithInfo.columns.get.columns

  "When there is a valid connection" should {
    "No mutation event for an invalid database or table" in {
      columnInfoActor ! fakeMutationWithNoInfo
      probe.expectNoMsg(connectionTimeout milliseconds)
    }

    "Reply with correct column info for a valid table with preload" in {
      columnInfoActor ! mutationWithNoInfo
      probe.expectMsg(
        connectionTimeout milliseconds,
        mutationWithInfo
      )
    }

    "Reply with correct column info for a valid table without preload" in {
      columnInfoActorNoPreload ! mutationWithNoInfo
      probeNoPreload.expectMsg(
        connectionTimeout milliseconds,
        mutationWithInfo
      )
    }

    "Reply with correct column info when a column is added, changed or dropped" in {
      var alterSql = "alter table changestream_test.users add column email varchar(128)"
      queryAndWait(alterSql)

      val alter1 = AlterTableEvent(database, tableName, alterSql)
      val m1ni = mutationWithNoInfo

      columnInfoActor ! alter1
      columnInfoActor ! m1ni

      inside(probe.expectMsgType[MutationWithInfo](connectionTimeout milliseconds)) {
        case MutationWithInfo(m, tx, cols, _, _) =>
          m should be(m1ni.mutation)
          tx should be(m1ni.transaction)
          cols should not be empty
          cols.get.database should be(database)
          cols.get.tableName should be(tableName)
          cols.get.columns should be(columns :+ Column("email", "varchar", false))
      }

      Thread.sleep(2000) // TODO this sucks -- have to wait for the db to settle because we currently haven't figured out how to handle back-to-back alter table statements
      alterSql = "alter table changestream_test.users change email email_address text"
      queryAndWait(alterSql)

      val alter2 = AlterTableEvent(database, tableName, alterSql)
      val m2ni = mutationWithNoInfo

      columnInfoActor ! alter2
      columnInfoActor ! m2ni

      inside(probe.expectMsgType[MutationWithInfo](connectionTimeout milliseconds)) {
        case MutationWithInfo(m, tx, cols, _, _) =>
          m should be(m2ni.mutation)
          tx should be(m2ni.transaction)
          cols should not be empty
          cols.get.database should be(database)
          cols.get.tableName should be(tableName)
          cols.get.columns should be(columns :+ Column("email_address", "text", false))
      }

      Thread.sleep(2000) // TODO this sucks -- have to wait for the db to settle because we currently haven't figured out how to handle back-to-back alter table statements
      alterSql = "alter table changestream_test.users drop column email_address"
      queryAndWait(alterSql)

      val alter3 = AlterTableEvent(database, tableName, alterSql)
      val m3ni = mutationWithNoInfo

      columnInfoActor ! alter3
      columnInfoActor ! m3ni

      inside(probe.expectMsgType[MutationWithInfo](connectionTimeout milliseconds)) {
        case MutationWithInfo(m, tx, cols, _, _) =>
          m should be(m3ni.mutation)
          tx should be(m3ni.transaction)
          cols should not be empty
          cols.get.database should be(database)
          cols.get.tableName should be(tableName)
          cols.get.columns should be(columns)
      }
    }
  }
}
