package changestream.persistence

import changestream.events.{MutationEvent, MutationWithInfo}
import com.amazonaws.ClientConfiguration
import com.amazonaws.services.sns.AmazonSNSAsyncClient
import com.amazonaws.services.sns.model.{CreateTopicResult, PublishResult}
import com.github.dwhjames.awswrap.sns.AmazonSNSScalaClient
import com.typesafe.config.Config
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.util.{Failure, Success}

object SnsJournalPlugin {
  def getTopic(mutation: MutationEvent, topic: String): String = {
    val database = mutation.database.replaceAll("[^a-zA-Z0-9\\-_]", "-")
    val tableName = mutation.tableName.replaceAll("[^a-zA-Z0-9\\-_]", "-")
    topic.replace("{database}", database).replace("{tableName}", tableName)
  }
}

class SnsJournalPlugin(config: Config) extends EmitterBase {
  protected val log = LoggerFactory.getLogger(getClass)

  protected val TIMEOUT = config.getInt("timeout")
  protected lazy val client = new AmazonSNSScalaClient(
    new AmazonSNSAsyncClient(
      new ClientConfiguration().
        withConnectionTimeout(TIMEOUT).
        withRequestTimeout(TIMEOUT)
    )
  )

  protected var topicArns = Map.empty[String, Future[CreateTopicResult]]

  override def asyncEmitPayload(persistenceId: String, payload: Any) = payload match {
    case MutationWithInfo(mutation, _, _, Some(message), _) =>
      val topic = SnsJournalPlugin.getTopic(mutation, persistenceId)
      log.debug(s"Received message for topic '${topic}': ${message}")
      send(topic, message).map(_ => ())
    case _ =>
      log.error(s"Received invalid message: The SnsJournalPlugin only supports MutationWithInfo payloads.")
      throw new UnsupportedOperationException("The SnsJournalPlugin only supports MutationWithInfo payloads.")
  }

  protected def send(topic: String, message: String): Future[PublishResult] = {
    topicArns = topicArns.get(topic) match {
      case Some(_) => topicArns
      case None => topicArns + (topic -> client.createTopic(topic))
    }

    val request = topicArns.get(topic).get.flatMap(topic =>
      client.publish(topic.getTopicArn, message)
    )

    request onComplete {
      case Success(result) =>
        log.debug(s"Successfully published message to ${topic} (messageId ${result.getMessageId})")
      case Failure(exception) =>
        log.error(s"Failed to publish to topic ${topic}.", exception)
    }

    request
  }
}
