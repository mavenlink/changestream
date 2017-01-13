package changestream.persistence

/**
  * Borrowed from Akka Persistence Internals
  * https://github.com/akka/akka/blob/master/akka-persistence/src/main/scala/akka/persistence/journal/inmem/InmemJournal.scala
  * (Akka is Apache Licensed)
  *
  * Copyright (C) 2009-2016 Lightbend Inc. <http://www.lightbend.com>
  */
import akka.persistence.journal.AsyncWriteJournal
import akka.persistence.{AtomicWrite, PersistentRepr}

import scala.collection.immutable
import scala.concurrent.Future
import scala.util.Try

/**
  * INTERNAL API.
  *
  * In-memory journal for testing purposes only.
  */
class InmemJournal extends AsyncWriteJournal with InmemMessages {
  override def asyncWriteMessages(messages: immutable.Seq[AtomicWrite]): Future[immutable.Seq[Try[Unit]]] = {
    for (w ← messages; p ← w.payload)
      add(p)
    Future.successful(Nil) // all good
  }

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    Future.successful(highestSequenceNr(persistenceId))
  }

  override def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(
    recoveryCallback: PersistentRepr ⇒ Unit): Future[Unit] = {
    val highest = highestSequenceNr(persistenceId)
    if (highest != 0L && max != 0L)
      read(persistenceId, fromSequenceNr, math.min(toSequenceNr, highest), max).foreach(recoveryCallback)
    Future.successful(())
  }

  def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = {
    val toSeqNr = math.min(toSequenceNr, highestSequenceNr(persistenceId))
    var snr = 1L
    while (snr <= toSeqNr) {
      delete(persistenceId, snr)
      snr += 1
    }
    Future.successful(())
  }
}

/**
  * INTERNAL API.
  */
trait InmemMessages {
  // persistenceId -> persistent message
  var messages = Map.empty[String, Vector[PersistentRepr]]
  var sequenceNr = Map.empty[String, Long]

  def add(p: PersistentRepr): Unit = {
    messages = messages + (messages.get(p.persistenceId) match {
      case Some(ms) ⇒ p.persistenceId → (ms :+ p)
      case None     ⇒ p.persistenceId → Vector(p)
    })
    sequenceNr = sequenceNr + (p.persistenceId → p.sequenceNr)
  }

  def update(pid: String, snr: Long)(f: PersistentRepr ⇒ PersistentRepr): Unit = messages = messages.get(pid) match {
    case Some(ms) ⇒ messages + (pid → ms.map(sp ⇒ if (sp.sequenceNr == snr) f(sp) else sp))
    case None     ⇒ messages
  }

  def delete(pid: String, snr: Long): Unit = messages = messages.get(pid) match {
    case Some(ms) ⇒ messages + (pid → ms.filterNot(_.sequenceNr == snr))
    case None     ⇒ messages
  }

  def read(pid: String, fromSnr: Long, toSnr: Long, max: Long): immutable.Seq[PersistentRepr] = messages.get(pid) match {
    case Some(ms) ⇒ ms.filter(m ⇒ m.sequenceNr >= fromSnr && m.sequenceNr <= toSnr).take(safeLongToInt(max))
    case None     ⇒ Nil
  }

  def highestSequenceNr(pid: String): Long = sequenceNr.getOrElse(pid, 0L)

  private def safeLongToInt(l: Long): Int =
    if (Int.MaxValue < l) Int.MaxValue else l.toInt
}
