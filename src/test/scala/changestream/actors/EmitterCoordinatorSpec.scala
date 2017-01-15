package changestream.actors

import java.io.File

import akka.actor.{ActorRefFactory, PoisonPill, Props}
import akka.testkit.{TestActorRef, TestProbe}
import changestream.actors.BinlogClient.Connect
import changestream.actors.EmitterCoordinator.{ResetPosition, StartChangeStream}
import changestream.events.FilePosition
import changestream.helpers.{Base, Config, Fixtures}

class EmitterCoordinatorSpec extends Base with Config {
  val snapshotDir = testConfig.getString("akka.persistence.snapshot-store.local.dir")
  val waitFor = 500

  val testPosition = Some(FilePosition("test", 1))
  val (mutation, _, _) = Fixtures.mutationWithInfo("insert", rowCount = 2, transactionInfo = false, columns = false, position = testPosition)

  val probe = TestProbe()
  var counter = 0
  var emitterCoordinatorActor = getNextActorRef
  def getNextActorRef = {
    counter += 1
    reloadCurrentActorRef()
  }
  def reloadCurrentActorRef(newProbe: TestProbe = probe) =
    TestActorRef(Props(classOf[EmitterCoordinator], (_: ActorRefFactory) => newProbe.ref, s"test-emitter-coordinator-${counter}"))

  after {
    emitterCoordinatorActor ! PoisonPill
    new File(snapshotDir).listFiles().foreach(_.delete())
    emitterCoordinatorActor = getNextActorRef
    Thread.sleep(waitFor) // needed so we make sure persistAsync finishes before we send the next message
  }

  "should start changestream from present time when there is no position stored" in {
    emitterCoordinatorActor ! StartChangeStream
    probe.expectMsg[Connect](Connect(None))
  }

  "should start changestream from a specified position where there is a position stored" in {
    emitterCoordinatorActor ! mutation.copy(formattedMessage = Some("{foo: 'bar'}"))
    Thread.sleep(waitFor) // needed so we make sure persistAsync finishes before we send the next message

    emitterCoordinatorActor ! StartChangeStream
    probe.expectMsg[Connect](Connect(testPosition))
  }

  "should reset position when receiving a reset command" in {
    emitterCoordinatorActor ! mutation.copy(formattedMessage = Some("{foo: 'bar'}"))
    Thread.sleep(waitFor) // needed so we make sure persistAsync finishes before we send the next message

    emitterCoordinatorActor ! StartChangeStream
    probe.expectMsg[Connect](Connect(testPosition))

    emitterCoordinatorActor ! ResetPosition

    emitterCoordinatorActor ! StartChangeStream
    probe.expectMsg[Connect](Connect(None))
  }

  "should recover last good position when actor restarts" in {
    emitterCoordinatorActor ! StartChangeStream
    probe.expectMsg[Connect](Connect(None))

    emitterCoordinatorActor ! mutation.copy(formattedMessage = Some("{foo: 'bar'}"))
    Thread.sleep(waitFor) // needed so we make sure persistAsync finishes before we send the next message
    emitterCoordinatorActor ! PoisonPill

    val probe2 = TestProbe()
    val emitterCoordinatorActor2 = reloadCurrentActorRef(probe2)
    Thread.sleep(waitFor) // needed so we make sure the actor starts

    emitterCoordinatorActor2 ! StartChangeStream
    probe2.expectMsg[Connect](Connect(testPosition))
  }
}
