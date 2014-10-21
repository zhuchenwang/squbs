package org.squbs.unicomplex.serviceproxyactor

import org.squbs.unicomplex.{ServiceProxy, WebContext}
import akka.actor.{ActorLogging, Actor, Props}
import spray.http.StatusCodes._
import spray.http.{ChunkedResponseStart, HttpRequest, HttpResponse}
import spray.http.HttpHeaders.RawHeader

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



