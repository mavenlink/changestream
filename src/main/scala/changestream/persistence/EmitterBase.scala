package changestream.persistence

import akka.persistence.AtomicWrite

import scala.collection.immutable
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

abstract class EmitterBase extends InmemJournal {
  protected implicit val ec = context.dispatcher

  /**
    * Asynchronously emit a payload (message) for a given persistenceId (topic).
    * Throw an exception for validation issues prior to creating the future if a message
    * must be rejected.
    *
    * @param persistenceId -- the unique id for the persistence actor
    * @param payload -- the object to emit
    * @return
    */
  protected def asyncEmitPayload(persistenceId: String, payload: Any): Future[Unit]

  /**
    * If the emitter can support atomic commit of multiple messages, override this method.
    * @param persistenceId -- the unique id for the persistence actor
    * @param payloads -- the object to emit
    * @return
    */
  protected def asyncEmitPayloads(persistenceId: String, payloads: immutable.Seq[Any]): Future[Unit] = {
    throw new UnsupportedOperationException("Atomic emission of multiple messages is not supported.")
  }

  override final def asyncWriteMessages(messages: immutable.Seq[AtomicWrite]): Future[immutable.Seq[Try[Unit]]] = {
    val emitFutures = Future.sequence(messages.map { write =>
      reverseTryFuture(Try {
        write.payload.length match {
          case 1 =>
            asyncEmitPayload(write.persistenceId, write.payload.head.payload)
          case _ =>
            asyncEmitPayloads(write.persistenceId, write.payload.map(_.payload))
        }
      })
    })

    emitFutures.map { results =>
      for (write <- messages; payload ← write.payload) {
        add(payload)
      }

      results
    }
  }

  private def reverseTryFuture[T](tf: Try[Future[T]]): Future[Try[T]] = {
    Future.fromTry(tf).
      map(f =>
        f.map[Try[T]](Success(_))
      ).
      recover { case ex:Throwable =>
        Future.successful(Failure(ex))
      }.
      flatMap(f => f)
  }
}
