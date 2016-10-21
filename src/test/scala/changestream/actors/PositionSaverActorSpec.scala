package changestream.actors

import java.io.File

import akka.actor.{ActorRefFactory, Props}
import akka.testkit.{TestActorRef, TestProbe}
import changestream.helpers.{Base, Config}
import changestream.actors.PositionSaverActor._
import com.typesafe.config.ConfigFactory
import changestream.helpers.Fixtures
import spray.json._
import DefaultJsonProtocol._


import scala.concurrent.duration._
import scala.io.Source
import scala.language.postfixOps

class PositionSaverActorSpec extends Base with Config {
  import PositionInfoProtocol._

  val randFile = s"/tmp/changestream-test-${Fixtures.randomWord(8)}.json"
  val tempFileConfig = ConfigFactory
    .parseString(s"""changestream.persist-to-file = \"${randFile}\"""")
    .withFallback(testConfig)

  val positionActor = TestActorRef(Props(classOf[PositionSaverActor], tempFileConfig.getConfig("changestream")))

  override def afterAll() = {
    val f = new java.io.File(randFile)
    f.delete()

    super.afterAll()
  }

  val startFromPosition = PositionInfo("foo", 1)

  def assertPosition(sequence: Long) = {
    if(new File(randFile).exists) {
      val jsonString = Source.fromFile(randFile)("UTF-8").mkString
      if(jsonString.length > 0) {
        val positionFromFile = jsonString.parseJson match {
          case v: JsValue =>
            Some(v.convertTo[PositionInfo])
          case _ =>
            None
        }

        positionFromFile.get.sequence should be(sequence)
      }
      else {
        throw new Exception("Position file couldn't be JSON parsed.")
      }
    }
    else {
      throw new Exception("Position file doesn't exist.")
    }
  }

  "When receiving an PositionInfo message" should {
    "Fail when there is no current position" in {
      positionActor ! PositionInfo("foo", 1)

      expectMsgType[akka.actor.Status.Failure](1000 milliseconds)
    }
  }
  "When receiving an Initialize message" should {

    "Set current position if we don't have a saved position" in {
      positionActor ! Initialize(startFromPosition)

      val startFrom = expectMsgType[StartFrom](1000 milliseconds)
      startFrom should be(StartFrom(startFromPosition))
    }
    "Restore saved position and return it if we do have a saved position" in {
      positionActor ! Initialize(startFromPosition)
      expectMsgType[StartFrom](1000 milliseconds)

      positionActor ! Initialize(PositionInfo("foo", 5))

      val startFrom = expectMsgType[StartFrom](1000 milliseconds)
      startFrom should be(StartFrom(startFromPosition))
    }
  }
  "When receiving position info in order" should {
    "Apply the update" in {
      positionActor ! PositionInfo("foo", 2)
      assertPosition(2)
      positionActor ! PositionInfo("foo", 3)
      assertPosition(3)
    }
  }
  "When receiving position info out of order" should {
    "Re-order the position info updates before saving" in {
      positionActor ! PositionInfo("foo", 5)
      assertPosition(3)
      positionActor ! PositionInfo("foo", 4)
      assertPosition(5)
    }
  }
  "When receiving position info out of order by more than 1" should {
    "Re-order the position info updates before saving" in {
      positionActor ! PositionInfo("foo", 8)
      assertPosition(5)
      positionActor ! PositionInfo("foo", 6)
      assertPosition(6)
      positionActor ! PositionInfo("foo", 7)
      assertPosition(8)
    }
  }

//  "When there is a current position" should {
//    ""
//  }

  // P => save position

  // 1 -> 2 -> 4 -> 3 -> 5
  // 1 = Nothing save, so save 1
  // 2 = last was 1, so save 2
  // 4 = last was 2.. buffer 4
  // 5 = last was 4, save 5
  // 3 = last was 2, so save 3, check buffer (pop 4 and save)
}
