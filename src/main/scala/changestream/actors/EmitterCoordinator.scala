//package changestream.actors
//
//import akka.actor._
//import akka.pattern.ask
//import akka.persistence._
//import changestream.events.MutationWithInfo
//
//import scala.util.{Failure, Success}
//
////case class CurrentPosition(position: Option[String] = None, buffer: List[String] = List.empty[String]) {
//////  def updated(evt: Evt): ExampleState = copy(evt.data :: events)
////  def bufferSize: Int = buffer.length
////  override def toString: String = position.getOrElse("")
////}
//
//class EmitterCoordinator extends PersistentActor {
//  override def persistenceId = "changestream-emmiter-coordinator"
//  val emitter = context.actorOf(Props(new SnsActor()), name = "emitterActor")
//
//  var state:Option[String] = None
//
//  def updateState(successfulPosition: Option[String]): Unit = {
//    state = successfulPosition
//    saveSnapshot(state)
//  }
//
//  def startBinlogProcessing = {
//    println(s"TODO Starting binlog at position ${state.getOrElse("null position")}")
//  }
//
////  def numEvents =
////    state.size
//
//  val receiveRecover: Receive = {
//    case position: Option[String] => updateState(position)
//    case SnapshotOffer(_, snapshot: Option[String]) => state = snapshot
//    case RecoveryCompleted => startBinlogProcessing
//  }
//
//  val receiveCommand: Receive = {
//    case MutationWithInfo(_, _, _, Some(message: String), position: Option[String]) =>
//      emitter.ask(message) onSuccess {
//        case success: akka.actor.Status.Success =>
//          persistAsync(position)(updateState)
//        case failure: akka.actor.Status.Failure =>
//          //TODO
//      }
//      // send to SNSActor
//      // with callback that invokes persist
//      // Is this compatible with persistAsync??
////    case "snap"  => saveSnapshot(state)
////    case "print" => println(state)
//  }
//}
