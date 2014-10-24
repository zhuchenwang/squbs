package org.squbs.unicomplex

import akka.actor._
import spray.http._
import scala.collection.mutable
import scala.util.Try
import spray.http.StatusCodes._
import scala.concurrent.Future
import java.util.concurrent.ConcurrentLinkedQueue
import spray.http.HttpRequest
import spray.can.Http.RegisterChunkHandler
import spray.http.Confirmed
import spray.http.ChunkedRequestStart
import scala.util.Failure
import spray.http.ChunkedResponseStart
import scala.Some
import spray.http.HttpResponse
import scala.util.Success

/**
 * Created by lma on 14-10-11.
 */
abstract class ServiceProxy[T](hostActorProps: Props) extends Actor with ActorLogging with HttpLifecycle[T] {

  val chunkMessageHandlers = mutable.WeakHashMap.empty[ActorRef, ChunkMessageHandler] // buffer chunked message

  val hostActor = context.actorOf(hostActorProps)

  import context.dispatcher

  def receive: Actor.Receive = {

    //forward msg from route definition
    case Initialized(report) =>
      context.parent ! Initialized(report)

    case req: HttpRequest =>
      val responder = sender() // must do it here, trust me!
      Future {
        handleRequest(req)
      } onComplete {
        case Success(result) =>
          val responseAgent = context.actorOf(Props(new ResponseAgent(responder, result._2)))
          hostActor tell(result._1, responseAgent)
        case Failure(t) =>
          log.error(t, "Failed to handle request in RequestAgent")
          val responseAgent = context.actorOf(Props(new ResponseAgent(responder, None)))
          responseAgent ! HttpResponse(InternalServerError, "Error in processing request.")
      }

    case crs: ChunkedRequestStart =>
      val chunkHandler = new ChunkMessageHandler // cheaper than an actor??
    val responder = sender()
      chunkMessageHandlers += responder -> chunkHandler

      Future {
        handleRequest(crs.request)
      } onComplete {
        case Success(result) =>
          val responseAgent = context.actorOf(Props(new ResponseAgent(responder, result._2)))
          hostActor tell(ChunkedRequestStart(result._1), responseAgent)
          chunkHandler.ready(responseAgent) // start sending chunks in buffer

        case Failure(t) =>
          log.error(t, "Failed to handle request in RequestAgent")
          val responseAgent = context.actorOf(Props(new ResponseAgent(responder, None)))
          responseAgent ! HttpResponse(InternalServerError, "Error in processing request.")
      }

    //below will not be called if user go with RegisterChunkHandler
    case chunk: MessageChunk =>
      chunkMessageHandlers.get(sender()) match {
        case Some(handler) => handler.bufferOrSend(chunk)
        case None => log.warning("No actor agent found. Possibly already timed out.")
      }

    case chunkEnd: ChunkedMessageEnd =>
      chunkMessageHandlers.get(sender()) match {
        case Some(handler) =>
          handler.bufferOrSend(chunkEnd)
          chunkMessageHandlers -= sender()
        case None => log.warning("No actor agent found. Possibly already timed out.")
      }

    //let underlying actor handle it
    case other => hostActor forward other

  }

  class ChunkMessageHandler {

    @volatile var responder: ActorRef = null

    val buffer = new ConcurrentLinkedQueue[HttpRequestPart] // make sure thread safe

    def ready(responseAgent: ActorRef) = {
      responder = responseAgent
      var msg: HttpRequestPart = buffer.poll()
      while (msg != null) {
        hostActor tell(msg, responder)
        msg = buffer.poll()
      }
    }

    def bufferOrSend(chunk: HttpRequestPart) = {
      if (responder == null) {
        buffer.offer(chunk)
      } else {
        hostActor tell(chunk, responder)
      }
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

      //intercept
      case rch@RegisterChunkHandler(handler) =>
        client ! RegisterChunkHandler(self)
        context.become(handleChunkRequest(handler), false)

      case other =>
        client forward other
    }


    def processChunkedResponseStart(crs: ChunkedResponseStart, converter: ChunkedResponseStart => Any): Unit = {
      Try {
        handleChunkedResponseStart(crs, reqCtx)
      } match {
        case Success(chunkedResponseStart) =>
          client forward converter(chunkedResponseStart)
          context.become(handleChunkResponse)
        case Failure(t) =>
          log.error(t, "Failed to handle response in ResponseAgent")
          client ! HttpResponse(InternalServerError, "Error in processing response.")
      }
    }

    def handleChunkRequest(chunkHandler: ActorRef): Actor.Receive = {
      case chunk: MessageChunk =>
        chunkHandler ! chunk

      case chunkEnd: ChunkedMessageEnd =>
        chunkHandler ! ChunkedMessageEnd
        chunkMessageHandlers -= sender()
        context.unbecome

    }

    def handleChunkResponse: Actor.Receive = {
      //TODO what if process ChunkedResponseStart failed?
      case chunk: MessageChunk =>
        client forward chunk

      case chunkEnd: ChunkedMessageEnd =>
        client forward chunkEnd // emit msg first
        handleChunkResponseEnd(reqCtx)
        context.stop(self)
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
