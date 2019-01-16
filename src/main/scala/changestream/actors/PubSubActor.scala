package changestream.actors

import java.util.concurrent.TimeUnit

import scala.language.postfixOps
import akka.actor.{Actor, ActorRef, ActorRefFactory}
import changestream.actors.PositionSaver.EmitterResult
import changestream.events.MutationWithInfo
import changestream.helpers.Topic
import com.google.cloud.pubsub.v1.{Publisher, TopicAdminClient}
import com.google.protobuf.ByteString
import com.google.pubsub.v1.{ProjectTopicName, PubsubMessage}
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory

import scala.collection.mutable

class PubSubActor(getNextHop: ActorRefFactory => ActorRef,
               config: Config = ConfigFactory.load().getConfig("changestream")) extends Actor {

  protected val nextHop = getNextHop(context)
  protected val log = LoggerFactory.getLogger(getClass)
  protected implicit val ec = context.dispatcher

  protected val gcpProject = config.getString("gcp.project")
  protected val pubsubTopic = config.getString("gcp.pubsub.topic")
  protected val TIMEOUT = config.getLong("gcp.timeout")
  protected val topicHasVariable = pubsubTopic.contains("{")

  protected val topicPublishers = mutable.HashMap.empty[String, Publisher]

  protected def getTopicPublisher(topic: String) = {
    //TODO async get or create
    val projectTopicName = ProjectTopicName.of(gcpProject, topic)
    val topicAdminClient = TopicAdminClient.create

    try {
      val topicInfo = topicAdminClient.getTopic(projectTopicName)
      log.info(s"Found topic ${projectTopicName}.")

      Publisher.newBuilder(topicInfo.getName).build
    }
    catch {
      // Gross.. exceptions in program flow
      case exception: com.google.api.gax.rpc.NotFoundException =>
        log.info(s"Couldn't find existing topic ${projectTopicName}, creating...")

        val topicInfo = topicAdminClient.createTopic(projectTopicName)
        log.info(s"Created topic ${projectTopicName}.")

        Publisher.newBuilder(topicInfo.getName).build
    }
    finally {
      topicAdminClient.close()
    }
  }

  def publish(publisher: Publisher, topic: String, message: String, pos: String) = {
    val data = ByteString.copyFromUtf8(message)
    val pubsubMessage = PubsubMessage.newBuilder.setData(data).build
    val messageIdJavaFuture = publisher.publish(pubsubMessage)

    val handler = new Thread {
      override def run {
        try {
          val messageId = messageIdJavaFuture.get(TIMEOUT, TimeUnit.SECONDS)
          log.debug(s"Successfully published message to ${publisher.getTopicNameString} (messageId ${messageId})")
          nextHop ! EmitterResult(pos)
        }
        catch {
          case exception: Exception =>
            log.error(s"Failed to publish to topic ${publisher.getTopicNameString}: ${exception.getMessage}")
            throw exception
            // TODO retry N times then exit
        }
      }
    }

    messageIdJavaFuture.addListener(handler, ec)
  }

  override def postStop = {
    topicPublishers.values.par.foreach { publisher =>
      log.info(s"Shutting down publisher for topic ${publisher.getTopicNameString}")

      publisher.shutdown()
      publisher.awaitTermination(1, TimeUnit.MINUTES)
    }
  }

  def receive = {
    case MutationWithInfo(mutation, pos, _, _, Some(message: String)) =>
      log.debug(s"Received message of size ${message.length}")
      log.trace(s"Received message: ${message}")

      val topic = Topic.getTopic(mutation, pubsubTopic, topicHasVariable)
      val topicPublisher = topicPublishers.getOrElse(topic, getTopicPublisher(topic))
      topicPublishers.update(topic, topicPublisher)

      publish(topicPublisher, topic, message, pos)
  }
}

