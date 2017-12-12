package changestream.actors

import java.util.UUID

import akka.actor.{ Actor, ActorRef, ActorRefFactory }
import changestream.events.MutationWithInfo

import collection.mutable
import changestream.events._
import org.slf4j.LoggerFactory

import com.newrelic.api.agent.Trace

class TransactionActor(getNextHop: ActorRefFactory => ActorRef) extends Actor {
  protected val log = LoggerFactory.getLogger(getClass)
  protected val nextHop = getNextHop(context)

  /** Mutable State! */
  protected var mutationCount: Long = 1
  protected var currentGtid: Option[String] = None
  protected var previousMutation: Option[MutationWithInfo] = None

  @Trace (dispatcher=true)
  def receive = {
    case BeginTransaction =>
      log.debug(s"Received BeginTransacton")
      mutationCount = 1
      currentGtid = Some(UUID.randomUUID.toString)
      previousMutation = None

    case Gtid(guid) =>
      log.debug(s"Received GTID for transaction: ${guid}")
      currentGtid = Some(guid)

    case event: MutationWithInfo =>
      log.debug(s"Received Mutation for tableId: ${event.mutation.tableId}")
      currentGtid match {
        case None =>
          nextHop ! event
        case Some(gtid) =>
          previousMutation.foreach { mutation =>
            log.debug(s"Adding transaction info and forwarding to the ${nextHop.path.name} actor")
            nextHop ! mutation
          }
          previousMutation = Some(event.copy(
            transaction = Some(TransactionInfo(
              gtid = gtid,
              currentRow = mutationCount
            ))
          ))
          mutationCount += event.mutation.rows.length
      }

    case _: TransactionEvent =>
      log.debug(s"Received Commit/Rollback")
      previousMutation.foreach { mutation =>
        log.debug(s"Adding transaction info and forwarding to the ${nextHop.path.name} actor")
        nextHop ! mutation.copy(
          transaction = mutation.transaction.map { txInfo =>
            txInfo.copy(lastMutationInTransaction = true)
          }
        )
      }
      mutationCount = 1
      currentGtid = None
      previousMutation = None
  }
}
