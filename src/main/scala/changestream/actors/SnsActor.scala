package changestream.actors

import scala.language.postfixOps
import scala.util.{Failure, Success}
import akka.actor.Actor
import changestream.events.{MutationEvent, MutationWithInfo}
import com.amazonaws.services.sns.AmazonSNSAsyncClient
import com.amazonaws.services.sns.model.CreateTopicResult
import com.github.dwhjames.awswrap.sns.AmazonSNSScalaClient
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.concurrent.Future

import com.newrelic.api.agent.Trace

object SnsActor {
  def getTopic(mutation: MutationEvent, topic: String, topicHasVariable: Boolean = true): String = {
    val database = mutation.database.replaceAll("[^a-zA-Z0-9\\-_]", "-")
    val tableName = mutation.tableName.replaceAll("[^a-zA-Z0-9\\-_]", "-")
    topicHasVariable match {
      case true => topic.replace("{database}", database).replace("{tableName}", tableName)
      case false => topic
    }
  }
}

class SnsActor(config: Config = ConfigFactory.load().getConfig("changestream")) extends Actor {
  protected val log = LoggerFactory.getLogger(getClass)
  protected implicit val ec = context.dispatcher

  protected val TIMEOUT = config.getLong("aws.timeout")

  protected val snsTopic = config.getString("aws.sns.topic")
  protected val snsTopicHasVariable = snsTopic.contains("{")

  protected val client = new AmazonSNSScalaClient(
    AmazonSNSAsyncClient.
      asyncBuilder().
      withRegion(config.getString("aws.region")).
      build().
      asInstanceOf[AmazonSNSAsyncClient]
  )
  protected val topicArns = mutable.HashMap.empty[String, Future[CreateTopicResult]]

  @Trace (dispatcher=true)
  def receive = {
    case MutationWithInfo(mutation, _, _, Some(message: String)) =>
      log.debug(s"Received message: ${message}")

      val origSender = sender()
      val topic = SnsActor.getTopic(mutation, snsTopic, snsTopicHasVariable)
      val topicArn = topicArns.getOrElse(topic, client.createTopic(topic))
      topicArns.update(topic, topicArn)

      val request = topicArn.flatMap(topic => client.publish(topic.getTopicArn, message))

      request onComplete {
        case Success(result) =>
          log.debug(s"Successfully published message to ${topic} (messageId ${result.getMessageId})")
          origSender ! akka.actor.Status.Success(result.getMessageId)
        case Failure(exception) =>
          log.error(s"Failed to publish to topic ${topic}: ${exception.getMessage}")
          throw exception
      }
  }
}
