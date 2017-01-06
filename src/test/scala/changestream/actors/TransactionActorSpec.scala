package changestream.actors

import akka.actor.{ActorRefFactory, Props}
import akka.testkit.{TestActorRef, TestProbe}
import changestream.events.MutationWithInfo
import changestream.events._
import changestream.helpers.{Base, Fixtures}

class TransactionActorSpec extends Base {
  val probe = TestProbe()
  val maker = (_: ActorRefFactory) => probe.ref
  val transactionActor = TestActorRef(Props(classOf[TransactionActor], maker))

  val GUID_LENGTH = 36
  val (mutation, _, _) = Fixtures.mutationWithInfo("insert", rowCount = 2, transactionInfo = false, columns = false, position = Some(FilePosition("test", 1)))
  val gtid = "9fc4cdc0-8f3b-11e6-a5b1-e39f73659fee:24"


  def expectValidTransactionActorOutput(mutation: MutationEvent, rowCount: Long = 1, guid: Option[String] = None) = {
    val event = probe.expectMsgType[MutationWithInfo]
    inside(event) {
      case MutationWithInfo(m, Some(transactionInfo), _, _, _) =>
        m should be(mutation)
        transactionInfo.guid.length should not be(0)
        guid.foreach(transactionInfo.guid should be(_))
        transactionInfo.rowCount should be(rowCount)
    }
  }

  "When receiving a TransactionEvent" should {
    "expect inTransaction to be true when we are in a transaction" in {
      transactionActor ! BeginTransaction

      transactionActor ! mutation
      probe.expectNoMsg

      transactionActor ! CommitTransaction(1)

      expectValidTransactionActorOutput(mutation.mutation, 2)
    }

    "expect inTransaction to be false when we are not in a transaction" in {
      transactionActor ! mutation

      probe.expectMsg[MutationWithInfo](mutation)
    }

    "expect inTransaction to be false after a rollback" in {
      transactionActor ! BeginTransaction
      transactionActor ! RollbackTransaction
      expectNoMsg

      transactionActor ! mutation

      probe.expectMsg[MutationWithInfo](mutation)
    }

    "expect inTransaction to be false after a commit" in {
      transactionActor ! BeginTransaction
      transactionActor ! CommitTransaction(1)
      expectNoMsg

      transactionActor ! mutation

      probe.expectMsg[MutationWithInfo](mutation)
    }

    "expect the same transaction id for mutations in the same transaction" in {
      transactionActor ! BeginTransaction

      transactionActor ! mutation
      transactionActor ! mutation

      transactionActor ! CommitTransaction(1)

      expectValidTransactionActorOutput(mutation.mutation, 4)
      expectValidTransactionActorOutput(mutation.mutation, 4)
    }

    "When receiving a GtidEvent event" should {
      "Expect the TransactionId to be overwritten with the gtid from MySQL" in {
        transactionActor ! BeginTransaction
        transactionActor ! Gtid(gtid)

        transactionActor ! mutation
        probe.expectNoMsg

        transactionActor ! CommitTransaction(1)

        expectValidTransactionActorOutput(mutation.mutation, 2, Some(gtid))
      }
    }

    "expect the binlog position not to change until the transaction is committed" in {
      val (mutation1, _, _) = Fixtures.mutationWithInfo("insert", rowCount = 2, transactionInfo = false, columns = false, sequenceNext = 1, position = Some(FilePosition("test", 4)))
      val (mutation2, _, _) = Fixtures.mutationWithInfo("insert", rowCount = 2, transactionInfo = false, columns = false, sequenceNext = 2, position = Some(FilePosition("test", 5)))

      transactionActor ! BeginTransaction
      transactionActor ! mutation1
      transactionActor ! mutation2
      transactionActor ! CommitTransaction(6)

      val txPosition1 = probe.expectMsgType[MutationWithInfo].position
      val txPosition2 = probe.expectMsgType[MutationWithInfo].position

      txPosition1 should be(None)
      txPosition2 should be(Some(FilePosition("test", 6)))
    }
  }
}
