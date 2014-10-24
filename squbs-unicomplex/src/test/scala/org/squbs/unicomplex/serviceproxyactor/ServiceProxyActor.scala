package org.squbs.unicomplex.serviceproxyactor

import org.squbs.unicomplex.{ServiceProxy, WebContext}
import akka.actor.{ActorRef, Props, ActorLogging, Actor}
import spray.http.StatusCodes._
import spray.http._
import spray.http.HttpMethods._
import spray.http.HttpRequest
import spray.http.HttpResponse
import spray.http.ChunkedRequestStart
import spray.http.HttpHeaders.RawHeader
import spray.http.ChunkedResponseStart
import scala.Some
import spray.can.Http.RegisterChunkHandler
import org.jvnet.mimepull.MIMEMessage
import java.io.ByteArrayInputStream
import org.apache.commons.io.IOUtils

/**
 * Created by lma on 14-10-13.
 */
class ServiceProxyActor extends Actor with WebContext with ActorLogging {

  def receive = {
    case req: HttpRequest =>
      val customHeader = req.headers.find(h => h.name.equals("dummyReqHeader"))
      val output = customHeader match {
        case None => "No custom header found"
        case Some(header) => header.value
      }
      sender() ! HttpResponse(OK, output)


    case start@ChunkedRequestStart(req@HttpRequest(POST, Uri.Path("/serviceproxyactor/file-upload"), _, _, _)) =>
      val handler = context.actorOf(Props(new ChunkHandler(sender, start)))
      sender() ! RegisterChunkHandler(handler)


  }

}

class ChunkHandler(client: ActorRef, start: ChunkedRequestStart) extends Actor with ActorLogging {

  import start.request._

  // client ! CommandWrapper(SetRequestTimeout(Duration.Inf))
  // cancel timeout
  val Some(HttpHeaders.`Content-Type`(ContentType(multipart: MultipartMediaType, _))) = header[HttpHeaders.`Content-Type`]
  val boundary = multipart.parameters("boundary")

  val content = new collection.mutable.ArrayBuffer[Byte]

  val modifiedHeader = headers.find(h => h.name.equals("dummyReqHeader")).get.value

  receivedChunk(entity.data)

  def receivedChunk(data: HttpData) {
    if (data.length > 0) {
      val byteArray = data.toByteArray
      //println("Received "+ChunkedRequestHandler.chunkCount +":"+ byteArray.length)
      content ++= byteArray
    }
  }

  def receive = {
    case chunk: MessageChunk => receivedChunk(chunk.data)

    case chunkEnd: ChunkedMessageEnd =>
      import collection.JavaConverters._

      val output = new collection.mutable.ArrayBuffer[Byte]
      new MIMEMessage(new ByteArrayInputStream(content.toArray), boundary).getAttachments.asScala.foreach(part => {
        // set the size for verification
        output ++= IOUtils.toByteArray(part.readOnce())

      })

      sender() ! HttpResponse(OK, HttpEntity(output.toArray), List(RawHeader("dummyReqHeader", modifiedHeader)))
      //      client ! CommandWrapper(SetRequestTimeout(2.seconds)) // reset timeout to original value
      context.stop(self)
    case _ =>
      println("unknown message")
  }


}


class DummyServiceProxyForActor(hostActorProps: Props) extends ServiceProxy[String](hostActorProps) {
  def handleRequest(req: HttpRequest): (HttpRequest, Option[String]) = {
    val newreq = req.copy(headers = RawHeader("dummyReqHeader", "PayPal") :: req.headers)
    (newreq, Option("CDC"))
  }

  def handleChunkResponseEnd(reqCtx: Option[String]): Unit = ???

  def handleResponse(resp: HttpResponse, reqCtx: Option[String]): HttpResponse = {
    resp.copy(headers = RawHeader("dummyRespHeader", reqCtx.get) :: resp.headers)
  }

  def handleChunkedResponseStart(crs: ChunkedResponseStart, reqCtx: Option[String]): ChunkedResponseStart = ???
}



