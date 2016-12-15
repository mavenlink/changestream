package changestream.actors

import akka.actor.SupervisorStrategy.Escalate

import scala.collection.mutable
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success}
import akka.actor.{Actor, ActorRef, Cancellable, OneForOneStrategy}
import changestream.events.MutationWithInfo
import com.amazonaws.services.sqs.AmazonSQSAsyncClient
import com.amazonaws.services.sqs.model.SendMessageBatchResult
import com.github.dwhjames.awswrap.sqs.AmazonSQSScalaClient
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory

import collection.JavaConverters._
import scala.concurrent.Await

object SqsActor {
  case class FlushRequest(origSender: ActorRef)
  case class BatchResult(queued: Seq[String], failed: Seq[String])
}

class SqsActor(config: Config = ConfigFactory.load().getConfig("changestream")) extends Actor {
  import SqsActor._

  override val supervisorStrategy =
    OneForOneStrategy(loggingEnabled = true) {
      case _: Exception => Escalate
    }

  protected val log = LoggerFactory.getLogger(getClass)
  protected implicit val ec = context.dispatcher

  protected val LIMIT = 10
  protected val MAX_WAIT = 250 milliseconds
  protected val TIMEOUT = config.getLong("aws.timeout")


  protected var cancellableSchedule: Option[Cancellable] = None
  protected def setDelayedFlush(origSender: ActorRef) = {
    val scheduler = context.system.scheduler
    cancellableSchedule = Some(scheduler.scheduleOnce(MAX_WAIT) { self ! FlushRequest(origSender) })
  }
  protected def cancelDelayedFlush = cancellableSchedule.foreach(_.cancel())

  protected val messageBuffer = mutable.ArrayBuffer.empty[String]
  protected def getMessageBatch: Seq[(String, String)] = {
    val batchId = Thread.currentThread.getId + "-" + System.nanoTime
    val messages = messageBuffer.zipWithIndex.map {
      case (message, index) => (s"${batchId}-${index}", message)
    }
    messageBuffer.clear()

    messages
  }

  protected val sqsQueue = config.getString("aws.sqs.queue")
  protected val client = new AmazonSQSScalaClient(new AmazonSQSAsyncClient(), ec)
  protected val queueUrl = client.createQueue(sqsQueue)
  queueUrl.failed.map {
    case exception:Throwable =>
      log.error(s"Failed to get or create SQS queue ${sqsQueue}.", exception)
      throw exception
  }

  override def preStart() = {
    val url = Await.result(queueUrl, TIMEOUT milliseconds)
    log.info(s"Connected to SNS topic ${sqsQueue} with ARN ${url}")
  }
  override def postStop() = cancelDelayedFlush

  def receive = {
    case MutationWithInfo(mutation, _, _, Some(message: String), _) =>
      log.debug(s"Received message: ${message}")

      cancelDelayedFlush

      messageBuffer += message
      messageBuffer.size match {
        case LIMIT => flush(sender())
        case _ => setDelayedFlush(sender())
      }

    case FlushRequest(origSender) =>
      flush(origSender)

    case _ =>
      log.error("Received invalid message")
      sender() ! akka.actor.Status.Failure(new Exception("Received invalid message"))
  }

  protected def flush(origSender: ActorRef) = {
    log.debug(s"Flushing ${messageBuffer.length} messages to SQS.")

    val request = for {
      url <- queueUrl
      req <- client.sendMessageBatch(
        url.getQueueUrl,
        getMessageBatch
      )
    } yield req

    request onComplete {
      case Success(result) =>
        log.debug(s"Successfully sent message batch to ${sqsQueue} " +
          s"(sent: ${result.getSuccessful.size()}, failed: ${result.getFailed.size()})")
        origSender ! akka.actor.Status.Success(getBatchResult(result))
      case Failure(exception) =>
        log.error(s"Failed to send message batch to ${sqsQueue}", exception)
        origSender ! akka.actor.Status.Failure(exception)
    }
  }

  protected def getBatchResult(result: SendMessageBatchResult) = {
    BatchResult(
      result.getSuccessful.asScala.map(_.getMessageId),
      result.getFailed.asScala.map(error => s"${error.getCode}: ${error.getMessage}")
    )
  }
}
