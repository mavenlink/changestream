package changestream.actors

import akka.actor.{ActorRefFactory, Props}
import akka.testkit.{TestActorRef, TestProbe}
import changestream.actors.PositionSaver.EmitterResult
import changestream.helpers.{Config, Emitter}
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._
import scala.language.postfixOps

class PubSubActorSpec extends Emitter with Config {
  val probe = TestProbe()
  val maker = (_: ActorRefFactory) => probe.ref

  val actorRef = TestActorRef(Props(classOf[PubSubActor], maker, gcpConfig))

  val configWithInterpolation = ConfigFactory.
    parseString("gcp.pubsub.topic = \"integration_tests-{database}-{tableName}\"").
    withFallback(gcpConfig)
  val pubsubWithInterpolation = TestActorRef(Props(classOf[PubSubActor], maker, configWithInterpolation))


  "When PubSubActor receives a single valid message" should {
    "Immediately publish the message to Pub/Sub" in {
      actorRef ! message

      val result = probe.expectMsgType[EmitterResult](5000 milliseconds)
      result.position should be(message.nextPosition)
    }
  }

  "When PubSubActor receives a message" should {
    "Should correctly publish the message when the topic contains interpolated database and/or tableName" in {
      pubsubWithInterpolation ! message

      val result = probe.expectMsgType[EmitterResult](5000 milliseconds)
      result.position should be(message.nextPosition)
    }
  }
}
