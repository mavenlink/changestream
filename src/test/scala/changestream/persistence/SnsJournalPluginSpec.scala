package changestream.persistence

import akka.testkit.{ImplicitSender, TestActorRef, TestProbe}
import changestream.helpers._
import akka.actor.Props
import changestream.actors.EmitterJournalTester
import changestream.actors.EmitterJournalTester.PersistFailed

class SnsJournalPluginSpec extends Emitter with ImplicitSender {
  val probe = TestProbe()
  val journalRef = TestActorRef(Props(new EmitterJournalTester("sns-journal-working", probe.ref, "__integration_tests")), "test1")
  val probe2 = TestProbe()
  val journalRef2 = TestActorRef(Props(new EmitterJournalTester("sns-journal-working", probe2.ref, "__integration_tests-{database}-{tableName}")), "test2")
  val probe3 = TestProbe()
  val journalRef3 = TestActorRef(Props(new EmitterJournalTester("sns-journal-broken", probe3.ref, "__integration_tests2")), "test3")

  "When SnsActor receives a message for a simple topic" should {
    "publish the message to SNS" in {
      journalRef ! message
      val messages = probe.receiveN(1)
      messages.head should be(message.position)
    }
  }

  "When SnsActor receives a message for a templated topic" should {
    "publish the message to SNS" in {

      journalRef2 ! message
      val messages = probe2.receiveN(1)
      messages.head should be(message.position)
    }
  }

  "When SnsActor receives an invalid message" should {
    "Reject journal write" in {
      journalRef ! INVALID_MESSAGE
      probe.expectMsgType[EmitterJournalTester.PersistRejected]
    }
  }

  "When SnsActor connect fails" should {
    "Fail to journal write" in {
      journalRef3 ! message
      probe3.expectMsgType[PersistFailed]
    }
  }
}
