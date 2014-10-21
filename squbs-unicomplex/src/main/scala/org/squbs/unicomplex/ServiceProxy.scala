package org.squbs.unicomplex

import akka.actor._
import spray.http._
import scala.collection.mutable
import scala.util.Try
import spray.http.StatusCodes._
import spray.http.HttpRequest
import spray.http.Confirmed
import spray.http.ChunkedRequestStart
import scala.util.Failure
import spray.http.ChunkedResponseStart
import scala.Some
import spray.http.HttpResponse
import scala.util.Success
import akka.actor.Terminated

/**
 * Created by lma on 14-10-11.
 */
abstract class ServiceProxy[T](hostActorProps: Props) extends Actor with ActorLogging with HttpLifecycle[T] {

  val responderAgents = mutable.Map.empty[ActorRef, ActorRef] // WeakHashMap is reliable?

  val hostActor = context.actorOf(hostActorProps)

  def receive: Actor.Receive = {

    //forward msg from route definition
    case Initialized(report) =>
      context.parent ! Initialized(report)

    case req: HttpRequest =>
      createActorAgent ! req

    case crs: ChunkedRequestStart =>
      val agent = createActorAgent
      agent ! crs
      responderAgents += sender() -> agent
      context.watch(sender())

    case chunk: MessageChunk =>
      responderAgents.get(sender()) match {
        case Some(agent) => agent ! chunk
        case None => log.warning("No actor agent found. Possibly already timed out.")
      }

    case chunkEnd: ChunkedMessageEnd =>
      responderAgents.get(sender()) match {
        case Some(agent) =>
          agent ! chunkEnd
          responderAgents -= sender()
          context.unwatch(sender())
        case None => log.warning("No actor agent found. Possibly already timed out.")
      }

    // just in case that ChunkedMessageEnd is not received
    case Terminated(responder) =>
      responderAgents.remove(responder) foreach (context.stop(_)) // most likely already stopped?
      context.unwatch(responder)

    //let underlying actor handle it
    case other => hostActor forward other

  }

  private def createActorAgent: ActorRef = {
    val responder = sender() // must do it here, trust me!
    val props = Props(new RequestAgent(responder))
    context.actorOf(props)
  }


  private class RequestAgent(client: ActorRef) extends Actor with ActorLogging {

    def receive: Actor.Receive = {

      case req: HttpRequest =>
        Try {
          handleRequest(req)
        } match {
          case Success(result) =>
            val responseAgent = context.actorOf(Props(new ResponseAgent(client, result._2)))
            hostActor tell(result._1, responseAgent)
          case Failure(t) =>
            log.error(t, "Failed to handle request in RequestAgent")
            val responseAgent = context.actorOf(Props(new ResponseAgent(client, None)))
            responseAgent ! HttpResponse(InternalServerError, "Error in processing request.")
        }
        readyToStop

      case crs: ChunkedRequestStart =>
        Try {
          handleRequest(crs.request)
        } match {
          case Success(result) =>
            val responseAgent = context.actorOf(Props(new ResponseAgent(client, result._2)))
            hostActor tell(ChunkedRequestStart(result._1), responseAgent)
            context.watch(client)
            context.become(onChunkedRequest(responseAgent) orElse onStop)
          case Failure(t) =>
            log.error(t, "Failed to handle request in RequestAgent")
            val responseAgent = context.actorOf(Props(new ResponseAgent(client, None)))
            responseAgent ! HttpResponse(InternalServerError, "Error in processing request.")
            readyToStop
        }

      //just in case
      case other =>
        log.error("Unexpected msg:" + other)
        context.stop(self)

    }

    private def readyToStop = {
      context.watch(client)
      context.become(onStop)
    }

    private def onChunkedRequest(responseAgent: ActorRef): Actor.Receive = {

      case chunk: MessageChunk =>
        hostActor tell(chunk, responseAgent)

      case chunkEnd: ChunkedMessageEnd =>
        hostActor tell(chunkEnd, responseAgent)

    }

    private def onStop: Actor.Receive = {
      case Terminated(responder) =>
        context.stop(self)
        context.unwatch(responder)

      //just in case
      case other =>
        log.error("Unexpected msg:" + other)
        context.stop(self)
    }


  }


  /**
   * proxy for response
   * @param client
   */
  private class ResponseAgent(client: ActorRef, reqCtx: Option[T]) extends Actor with ActorLogging {

    // no need to watch client since parent will watch it and stop for sure

    def receive: Actor.Receive = {

      case resp: HttpResponse =>
        processResponse(resp)
        context.stop(self)

      case Confirmed(ChunkedResponseStart(resp), ack) =>
        processChunkedResponseStart(ChunkedResponseStart(resp), r => Confirmed(r, ack))

      // need it?
      case crs: ChunkedResponseStart =>
        processChunkedResponseStart(crs, r => r)

      //TODO what if process ChunkedResponseStart failed?
      case chunk: MessageChunk =>
        client forward chunk

      case chunkEnd: ChunkedMessageEnd =>
        client forward chunkEnd // emit msg first
        handleChunkResponseEnd(reqCtx)
        context.stop(self)

      case other =>
        client forward other
    }


    def processChunkedResponseStart(crs: ChunkedResponseStart, converter: ChunkedResponseStart => Any): Unit = {
      Try {
        handleChunkedResponseStart(crs, reqCtx)
      } match {
        case Success(chunkedResponseStart) =>
          client forward converter(chunkedResponseStart)
        case Failure(t) =>
          log.error(t, "Failed to handle response in ResponseAgent")
          client ! HttpResponse(InternalServerError, "Error in processing response.")
      }
    }

    def processResponse(resp: HttpResponse): Unit = {
      Try {
        handleResponse(resp, reqCtx)
      } match {
        case Success(response) =>
          client forward response
        case Failure(t) =>
          log.error(t, "Failed to handle response in ResponseAgent")
          client ! HttpResponse(InternalServerError, "Error in processing response.")
      }
    }


  }


}

/**
 * When doing request processing, we might need to store some additional data to be used in response processing
 * So we give user a chance to produce a byproduct in addition to HttpRequest and save it in the responseAgent
 * @tparam T
 */
trait HttpLifecycle[T] {

  def handleRequest(req: HttpRequest): (HttpRequest, Option[T])

  def handleResponse(resp: HttpResponse, reqCtx: Option[T]): HttpResponse

  def handleChunkedResponseStart(crs: ChunkedResponseStart, reqCtx: Option[T]): ChunkedResponseStart

  def handleChunkResponseEnd(reqCtx: Option[T]): Unit

}
