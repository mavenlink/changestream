package changestream.actors

import akka.actor.Actor
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory
import spray.json._
import DefaultJsonProtocol._

import scala.io.Source
import java.io.{File, PrintWriter}

import scala.collection.mutable

object PositionSaverActor {
  case class Initialize(positionOnConnect: PositionInfo)
  case class StartFrom(position: PositionInfo)
  case class PositionInfo(position: String, sequence: Long)

  object PositionInfoProtocol extends DefaultJsonProtocol {
    implicit val positionFormat = jsonFormat2(PositionInfo)
  }
}

class PositionSaverActor(config: Config = ConfigFactory.load().getConfig("changestream")) extends Actor {
  import PositionSaverActor._
  import PositionInfoProtocol._

  protected val log = LoggerFactory.getLogger(getClass)
  protected val persistToFile = config.getString("persist-to-file")

  protected var currentPosition: Option[PositionInfo] = None
  protected val mutationBuffer = mutable.HashMap.empty[Long, PositionInfo]

  override def preStart() = {
    if(new File(persistToFile).exists) {
      val jsonString = Source.fromFile(persistToFile)("UTF-8").mkString
      if(jsonString.length > 0) {
        currentPosition = jsonString.parseJson match {
          case v: JsValue =>
            Some(v.convertTo[PositionInfo])
          case _ =>
            None
        }
      }
    }

    super.preStart()
  }

  def receive = {
    case _: PositionInfo if currentPosition == None =>
      sender() ! akka.actor.Status.Failure(new Exception("You must Initialize the PositionSaverActor with position info."))

    case Initialize(p) if currentPosition == None =>
      persistInfo(p)
      sender() ! StartFrom(p)

    case Initialize(p) =>
      sender() ! StartFrom(currentPosition.get)

    case p: PositionInfo =>
      if(p.sequence > currentPosition.get.sequence + 1) {
        mutationBuffer(p.sequence) = p
      }
      else if(p.sequence > currentPosition.get.sequence) {
        var latestPosition = p
        while(mutationBuffer.contains(latestPosition.sequence + 1)) {
          latestPosition = mutationBuffer.remove(latestPosition.sequence + 1).get
        }
        persistInfo(latestPosition)
      }
  }

  def persistInfo(p: PositionInfo) = {
    val jsonString = p.toJson.compactPrint
    log.debug(s"Saving position: ${jsonString}")

    val positionWriter = new PrintWriter(persistToFile)
    positionWriter.write(jsonString)
    positionWriter.close

    currentPosition = Some(p)
  }
}
