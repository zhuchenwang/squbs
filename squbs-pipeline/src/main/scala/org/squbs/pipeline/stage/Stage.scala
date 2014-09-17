package org.squbs.pipeline.stage

import akka.actor._
import akka.pattern._

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

/**
 * Created by zhuwang on 8/20/14.
 */
case class ContextWrapper[ID, CTX](ctx: CTX, promise: Promise[CTX], transitionMap: Map[ID, Stage[ID, CTX]])

case class Stage[ID, CTX](id: ID, next: Option[ID])
                         (
                           doWork: (CTX, ActorContext) => Future[CTX],
                           entryTransition: CTX => Option[ID] = {x: CTX => None},
                           exitTransition: CTX => Option[ID] = {x: CTX => next},
                           beforeReceive: CTX => Unit = {x: CTX => },
                           afterReceive: CTX => Unit = {x: CTX => }
                         )
                         (implicit system: ActorSystem) {

  implicit val executionContext: ExecutionContext = system.dispatcher

  def withEntryTransition(fn: CTX => Option[ID]): Stage[ID, CTX] = {
    this.copy()(doWork, fn, exitTransition, beforeReceive, afterReceive)(system)
  }
  def withExitTransition(fn: CTX => Option[ID]): Stage[ID, CTX] = {
    this.copy()(doWork, entryTransition, fn, beforeReceive, afterReceive)(system)
  }

  def aroundReceive(before: CTX => Unit, after: CTX => Unit): Stage[ID, CTX] = {
    this.copy()(doWork, entryTransition, exitTransition, before, after)(system)
  }

  def process(input: CTX, p: Promise[CTX], stagesMap: Map[ID, Stage[ID, CTX]]): Unit = {
    system.actorOf(Props(new StageActor)) ! ContextWrapper(input, p, stagesMap)
  }

  private class StageActor extends Actor {
    override def receive: Actor.Receive = {
      case  ContextWrapper(ctx: CTX, p, stagesMap) => println(s"Entered $id")
        beforeReceive(ctx)
        Try(entryTransition(ctx)) match {
        case Success(Some(transId)) => println(s"$id -> $transId")
          stagesMap.get(transId) match {
            case Some(stage) => stage.process(ctx, p, stagesMap - transId)
            case None => p.failure(new Throwable(s"Cannot find Stage: $transId"))
          }
          context stop self
        case Success(None) => doWork(ctx, context) pipeTo self
          context.become({
            case processedCtx: CTX =>
              beforeReceive(ctx)
              Try(exitTransition(processedCtx)) match {
                case Success(Some(nextId)) => println(s"$id -> $nextId")
                  stagesMap.get(nextId) match {
                    case Some(stage) => stage.process(processedCtx, p, stagesMap - nextId)
                    case None => p.failure(new Throwable(s"Cannot find Stage: $nextId"))
                  }
                case Success(None) => p.success(processedCtx)
                case Failure(e) => p.failure(e)
              }
              afterReceive(ctx)
              context stop self
            case Status.Failure(e) => p.failure(e)
              context stop self
          })
          afterReceive(ctx)
        case Failure(e) => p.failure(e)
          context stop self
      }
    }
  }
}