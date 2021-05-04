package changestream.actors

import scala.language.postfixOps
import scala.util.{Failure, Success}
import akka.actor.{Actor, ActorRef, ActorRefFactory}
import changestream.actors.PositionSaver.EmitterResult
import changestream.events.{MutationEvent, MutationWithInfo}
import com.amazonaws.services.sns.AmazonSNSAsyncClient
import com.amazonaws.services.sns.model.CreateTopicResult
import com.github.dwhjames.awswrap.sns.AmazonSNSScalaClient
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory
import kamon.Kamon

import scala.collection.mutable
import scala.concurrent.Future

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

class SnsActor(getNextHop: ActorRefFactory => ActorRef,
               config: Config = ConfigFactory.load().getConfig("changestream")) extends Actor {

  protected val nextHop = getNextHop(context)
  protected val log = LoggerFactory.getLogger(getClass)
  protected val successMetric = Kamon.counter("changestream.emitter.total").refine("emitter" -> "sns", "result" -> "success")
  protected val failureMetric = Kamon.counter("changestream.emitter.total").refine("emitter" -> "sns", "result" -> "failure")
  protected val inFlightMetric = Kamon.rangeSampler("changestream.emitter.in_flight").refine("emitter" -> "sns")

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

  protected def getOrCreateTopic(topic: String) = {
    val ret = client.createTopic(topic)
    ret onComplete {
      case Success(topicResult) =>
        log.info(s"Ready to publish messages to SNS topic ${topic} (${topicResult.getTopicArn}).")
      case Failure(exception) =>
        log.error(s"Failed to find/create SNS topic ${topic}.", exception.getMessage)
        throw exception
    }
    ret
  }

  override def postStop = {
    import java.util.concurrent.TimeUnit
    // attempt graceful shutdown
    val executor = client.client.getExecutorService()
    executor.shutdown()
    executor.awaitTermination(60, TimeUnit.SECONDS)
    client.client.shutdown()
  }

  def receive = {
    case MutationWithInfo(mutation, pos, _, _, Some(message: String)) =>
      log.debug("Received message of size {}", message.length)
      log.trace("Received message: {}", message)

      val topic = SnsActor.getTopic(mutation, snsTopic, snsTopicHasVariable)
      val topicArn = topicArns.getOrElse(topic, getOrCreateTopic(topic))
      topicArns.update(topic, topicArn)

      inFlightMetric.increment()

      val request = topicArn.flatMap(topic => client.publish(topic.getTopicArn, message))

      request onComplete {
        case Success(result) =>
          inFlightMetric.decrement()

          log.info(s"Successfully published message to ${topic} (messageId ${result.getMessageId})")
          successMetric.increment()
          nextHop ! EmitterResult(pos)
        case Failure(exception) =>
          inFlightMetric.decrement()

          log.error(s"Failed to publish to topic ${topic}.", exception.getMessage)
          failureMetric.increment()
          throw exception
      }
  }
}
