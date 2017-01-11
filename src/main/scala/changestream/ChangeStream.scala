package changestream

import java.io.IOException

import akka.actor.{ActorSystem, Props}
import com.github.shyiko.mysql.binlog.BinaryLogClient
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.language.postfixOps
import akka.pattern.{after, ask}
import akka.io.IO
import akka.util.Timeout
import changestream.actors.ControlInterfaceActor
import changestream.events.{BinlogPosition, FilePosition}
import spray.can.Http

object ChangeStream extends App {
  protected val log = LoggerFactory.getLogger(getClass)
  implicit val system = ActorSystem("changestream")

  protected val config = ConfigFactory.load().getConfig("changestream")
  protected val mysqlHost = config.getString("mysql.host")
  protected val mysqlPort = config.getInt("mysql.port")
  protected lazy val client = new BinaryLogClient(
    mysqlHost,
    mysqlPort,
    config.getString("mysql.user"),
    config.getString("mysql.password")
  )

  @volatile
  protected var isPaused = false

  protected val host = config.getString("control.host")
  protected val port = config.getInt("control.port")
  protected val controlActor = system.actorOf(Props[ControlInterfaceActor], "control-interface")
  protected implicit val ec = system.dispatcher
  protected implicit val timeout = Timeout(10 seconds)

  /** Gracefully handle application shutdown from
    *  - Normal program exit
    *  - TERM signal
    *  - System reboot/shutdown
    */
  sys.addShutdownHook({
    log.info("Shutting down...")

    disconnect()
    controlActor ! Http.Unbind

    terminateActorSystemAndWait
  })

  /** Start the HTTP server for status and control **/
  protected val controlFuture = IO(Http).ask(Http.Bind(listener = controlActor, interface = host, port = port)).map {
    case Http.Bound(address) =>
      println(s"Control interface bound to ${address}")
    case Http.CommandFailed(cmd) =>
      println(s"Control interface could not bind to ${host}:${port}, ${cmd.failureMessage}")

      terminateActorSystemAndWait
      System.exit(2)
  }
  Await.result(controlFuture, 5000 milliseconds)

  /** Every changestream instance must have a unique server-id.
    *
    * http://dev.mysql.com/doc/refman/5.7/en/replication-setup-slaves.html#replication-howto-slavebaseconfig
    */
  client.setServerId(config.getLong("mysql.server-id"))

  /** If we lose the connection to the server retry every `changestream.mysql.keepalive` milliseconds. **/
  client.setKeepAliveInterval(config.getLong("mysql.keepalive"))

  /** Register the objects that will receive `onEvent` calls and deserialize data **/
  ChangeStreamEventListener.setConfig(config)
  client.registerEventListener(ChangeStreamEventListener)
  client.setEventDeserializer(ChangestreamEventDeserializer)

  /** Register the object that will receive BinaryLogClient connection lifecycle events **/
  client.registerLifecycleListener(ChangeStreamLifecycleListener)

  getConnected

  def serverName = s"${mysqlHost}:${mysqlPort}"
  def clientId = client.getServerId

  def isConnected = client.isConnected

  def setPosition(position: BinlogPosition) = {
    println(s"position is $position")
  }

  def connect() = {
    if(!client.isConnected()) {
      isPaused = false

      Future { getConnected }

      true
    }
    else {
      false
    }
  }

  def disconnect() = {
    if(client.isConnected()) {
      isPaused = true

      client.disconnect()

      true
    }
    else {
      false
    }
  }

  def reset() = {
    if(!client.isConnected()) {
      client.setBinlogFilename(null) //scalastyle:ignore
      true
    }
    else {
      false
    }
  }

  def stop() = {
    log.info("Shutting down...")

    disconnect()
    controlActor ! Http.Unbind

    terminateActorSystemAndWait
  }

  protected def terminateActorSystemAndWait = {
    system.terminate()
    Await.result(system.whenTerminated, 60 seconds)
  }

  protected def getConnected = {
    /** Finally, signal the BinaryLogClient to start processing events **/
    log.info(s"Starting changestream...")
    while(!isPaused && !client.isConnected) {
      try {
        client.connect()
      }
      catch {
        case e: IOException =>
          log.error(e.getMessage)
          log.error("Failed to connect to MySQL to stream the binlog, retrying...")
          Thread.sleep(5000)
        case e: Exception =>
          log.error("Failed to connect, exiting.", e)
          terminateActorSystemAndWait
          sys.exit(1)
      }
    }
  }
}
