package changestream.persistence

import akka.actor.{Actor, ActorRef, Cancellable}
import changestream.events.{MutationEvent, MutationWithInfo}
import com.amazonaws.ClientConfiguration
import com.amazonaws.services.sqs.AmazonSQSAsyncClient
import com.amazonaws.services.sqs.model.{CreateQueueResult, SendMessageBatchResult, SendMessageResult}
import com.github.dwhjames.awswrap.sqs.AmazonSQSScalaClient
import com.typesafe.config.Config
import org.slf4j.LoggerFactory

import scala.collection.immutable
import scala.concurrent.{Await, Future, Promise}
import scala.util.{Failure, Success}
import scala.concurrent.duration._
import scala.language.postfixOps

object SqsJournalPlugin {
  case class BatchResult(queued: Seq[String], failed: Seq[String])

  def getQueueName(mutation: MutationEvent, queueNameTemplate: String): String =
    SnsJournalPlugin.getTopic(mutation, queueNameTemplate)
}

class SqsJournalPlugin(config: Config) extends EmitterBase {
  import SqsJournalPlugin._

  protected val log = LoggerFactory.getLogger(getClass)

  protected val LIMIT = config.getInt("batch-size")
  protected val TIMEOUT = config.getInt("timeout")

  protected lazy val client = new AmazonSQSScalaClient(
    new AmazonSQSAsyncClient(
      new ClientConfiguration().
        withConnectionTimeout(TIMEOUT).
        withRequestTimeout(TIMEOUT)
    ),
    ec
  )

  protected var queueUrls = immutable.Map.empty[String, Future[CreateQueueResult]]

  override def asyncEmitPayload(persistenceId: String, payload: Any) = payload match {
    case MutationWithInfo(mutation, _, _, Some(message), _) =>
      val topic = getQueueName(mutation, persistenceId)
      log.debug(s"Received message for topic '${topic}': ${message}")
      send(topic, message).map(_ => ())
    case _ =>
      log.error(s"Received invalid message: The SqsJournalPlugin only supports MutationWithInfo payloads.")
      throw new UnsupportedOperationException("The SqsJournalPlugin only supports MutationWithInfo payloads.")
  }

  protected def send(queueName: String, message: String): Future[SendMessageResult] = {
    queueUrls = queueUrls.get(queueName) match {
      case Some(_) => queueUrls
      case None => queueUrls + (queueName -> client.createQueue(queueName))
    }

    val request = queueUrls.get(queueName).get.flatMap(queue =>
      client.sendMessage(queue.getQueueUrl, message)
    )

    request onComplete {
      case Success(result) =>
        log.debug(s"Successfully published message to ${queueName} (messageId ${result.getMessageId})")
      case Failure(exception) =>
        log.error(s"Failed to publish to queue ${queueName}.", exception)
    }

    request
  }

//  //TODO
//  protected def getMessageBatch: Seq[(String, String)] = {
//    val batchId = Thread.currentThread.getId + "-" + System.nanoTime
//    val messages = messageBuffer.zipWithIndex.map {
//      case ((message, _), index) => (s"${batchId}-${index}", message)
//    }
//    messages
//  }

//  protected def getBatchResult(result: SendMessageBatchResult) = {
//    BatchResult(
//      result.getSuccessful.asScala.map(_.getMessageId),
//      result.getFailed.asScala.map(error => s"${error.getCode}: ${error.getMessage}")
//    )
//  }
}
