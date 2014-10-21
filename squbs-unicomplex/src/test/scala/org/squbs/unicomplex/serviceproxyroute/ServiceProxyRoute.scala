package org.squbs.unicomplex.serviceproxyroute

import org.squbs.unicomplex.{ServiceProxy, WebContext, RouteDefinition}
import spray.routing.Directives._
import spray.http.{ChunkedResponseStart, HttpRequest, HttpEntity, HttpResponse}
import spray.http.MediaTypes._
import akka.actor.Props
import spray.http.HttpHeaders.RawHeader

/**
 * Created by lma on 14-10-13.
 */
class ServiceProxyRoute extends RouteDefinition with WebContext {
  def route = path("msg" / Segment) {
    param =>
      get {
        ctx =>
          val customHeader = ctx.request.headers.find(h => h.name.equals("dummyReqHeader"))
          val output = customHeader match {
            case None => "No custom header found"
            case Some(header) => header.value
          }
          ctx.responder ! HttpResponse(entity = HttpEntity(`text/plain`, param + output))
      }
  }
}

class DummyServiceProxyForRoute(hostActorProps: Props) extends ServiceProxy[String](hostActorProps) {
  def handleRequest(req: HttpRequest): (HttpRequest, Option[String]) = {
    val newreq = req.copy(headers = RawHeader("dummyReqHeader", "eBay") :: req.headers)
    (newreq, Some("CCOE"))
  }

  def handleChunkResponseEnd(reqCtx: Option[String]): Unit = ???

  def handleResponse(resp: HttpResponse, reqCtx: Option[String]): HttpResponse = {
    resp.copy(headers = RawHeader("dummyRespHeader", reqCtx.get) :: resp.headers)
  }

  def handleChunkedResponseStart(crs: ChunkedResponseStart, reqCtx: Option[String]): ChunkedResponseStart = ???
}



