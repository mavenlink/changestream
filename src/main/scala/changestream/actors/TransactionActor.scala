package changestream.actors

import java.util.UUID

import akka.actor.{ Actor, ActorRef, ActorRefFactory }
import changestream.events.MutationWithInfo

import collection.mutable
import changestream.events._
import org.slf4j.LoggerFactory

class TransactionActor(getNextHop: ActorRefFactory => ActorRef) extends Actor {
  protected val log = LoggerFactory.getLogger(getClass)
  protected val nextHop = getNextHop(context)

  /** Mutable State! */
  protected val mutationBuffer = mutable.ListBuffer[MutationWithInfo]()
  protected var inTransaction = false
  protected var transactionId: Option[String] = None

  def receive = {
    case BeginTransaction =>
      log.debug(s"Received BeginTransacton")

      inTransaction = true

    case Gtid(gtid) =>
      log.debug(s"Received GTID for transaction: ${gtid}")
      transactionId = Some(gtid)

    case CommitTransaction(nextPosition) =>
      log.debug(s"Received CommitTransacton with next position $nextPosition")

      lazy val txid = transactionId.getOrElse(UUID.randomUUID.toString)
      lazy val transactionInfo = TransactionInfo(txid, mutationBuffer.view.map(_.mutation.rows.length).sum)

      log.debug(s"Adding transaction info and forwarding ${mutationBuffer.length} events to the ${nextHop.path.name} actor")

      val lastEventSequenceNumber = mutationBuffer.last.mutation.sequence
      mutationBuffer.foreach(event => {
        if(event.mutation.sequence == lastEventSequenceNumber) {
          nextHop ! event.copy(transaction = Some(transactionInfo), position = Some(FilePosition(event.position.get.asInstanceOf[FilePosition].file, nextPosition)))
        }
        else {
          nextHop ! event.copy(transaction = Some(transactionInfo), position = None)
        }
      })

      inTransaction = false
      mutationBuffer.clear()

    case RollbackTransaction =>
      log.debug(s"Received RollbackTransacton")

      inTransaction = false
      mutationBuffer.clear()

    case event: MutationWithInfo =>
      log.debug(s"Received Mutation for tableId: ${event.mutation.tableId}")

      inTransaction match {
        case false =>
          log.debug(s"Forwarding mutation with no transaction to the ${nextHop.path.name} actor")
          nextHop ! event
        case true =>
          mutationBuffer += event
      }

    case _ =>
      throw new Exception("Invalid message received by TransactionActor")
  }
}
