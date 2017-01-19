package changestream.actors

import akka.actor.Actor
import changestream.ChangeStream
import changestream.events.BinlogPosition
import org.slf4j.LoggerFactory

object BinlogClient {
  case class Connect(position: Option[BinlogPosition])
  case object Disconnect
}

class BinlogClient extends Actor {
  import changestream.actors.BinlogClient._
  protected val log = LoggerFactory.getLogger(getClass)

  def receive = {
    case Connect(_) if ChangeStream.isConnected =>
      log.error("Received Connect message while connected")
    case Disconnect if !ChangeStream.isConnected =>
      log.error("Received Disconnect message while not connected")

    case Connect(Some(position)) =>
      ChangeStream.setPosition(position)
      ChangeStream.getConnected

    case Connect(None) =>
      ChangeStream.reset()
      ChangeStream.getConnected

    case Disconnect =>
      ChangeStream.disconnect()
  }
}
