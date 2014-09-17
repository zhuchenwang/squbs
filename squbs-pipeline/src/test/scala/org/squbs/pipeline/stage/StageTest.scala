package org.squbs.pipeline.stage

import akka.actor.{ActorContext, Status, ActorSystem}
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.{Matchers, FlatSpecLike}

import scala.collection.concurrent.TrieMap
import scala.concurrent.{Promise, ExecutionContext, Future}
import akka.pattern._

import scala.util.Try

/**
 * Created by zhuwang on 8/25/14.
 */
class StageTest extends TestKit(ActorSystem("StageTest"))
  with FlatSpecLike with Matchers with ImplicitSender {

  import org.squbs.pipeline.util.StageTransitionUtil._

  implicit val executionContext: ExecutionContext = system.dispatcher

  val stageDriver = StageFactory[String, String] {
    "a" ~> "b" ~> "c"
  } createStageDriver {(id, next) =>
    id match {
      case "a" =>
        new Stage[String, String](id, next)((msg, ctx) =>
          Future{msg + "a"}
        ).withExitTransition {msg =>
          msg match {
            case m if m.startsWith("exitTransition:") => Some("c")
            case _ => Some("b")
          }
        }
      case "b" =>
        new Stage[String, String](id, next)((msg, ctx) =>
          Future{msg + "b"}
        ).withEntryTransition {msg =>
          msg match {
            case m if m.startsWith("entryTransition:") => Some("c")
            case _ => None
          }
        }
      case "c" => new Stage[String, String](id, next)((msg, ctx) =>
        Future{msg + "c"}
      )
    }
  }

  "StageDriver" should "transit correctly if defined enter transition" in {
    stageDriver.process("hello ") pipeTo self
    expectMsg("hello abc")

    stageDriver.process("entryTransition:") pipeTo self
    expectMsg("entryTransition:ac")
  }

  "StageDriver" should "transit correctly if defined exit transition" in {
    stageDriver.process("hello ") pipeTo self
    expectMsg("hello abc")

    stageDriver.process("exitTransition:") pipeTo self
    expectMsg("exitTransition:ac")
  }

  "StageDriver" should "fail if exception happens during invocation" in {
    val sd = StageFactory[String, String] {
      "a" ~> "b"
    } createStageDriver {(id, next) =>
      id match {
        case "a" =>
          new Stage[String, String](id, next)((msg, ctx) =>
            Future{msg + "a"}
          )
        case "b" =>
          new Stage[String, String](id, next)((msg, ctx) =>
            Future{throw new Exception("error")}
          )
      }
    }
    sd.process("hello") pipeTo self
    expectMsgType[Status.Failure]
  }

  "StageDriver" should "fail if exception happens during entry transition" in {
    val sd = StageFactory[String, String] {
      "a" ~> "b"
    } createStageDriver {(id, next) =>
      id match {
        case "a" =>
          new Stage[String, String](id, next)((msg, ctx) =>
            Future{msg + "a"}
          ).withEntryTransition(_ => throw new Exception("error"))
        case "b" =>
          new Stage[String, String](id, next)((msg, ctx) =>
            Future{msg + "b"}
          )
      }
    }
    sd.process("hello") pipeTo self
    expectMsgType[Status.Failure]
  }

  "StageDriver" should "fail if exception happens during exit transition" in {
    val sd = StageFactory[String, String] {
      "a" ~> "b"
    } createStageDriver {(id, next) =>
      id match {
        case "a" =>
          new Stage[String, String](id, next)((msg, ctx) =>
            Future{msg + "a"}
          ).withExitTransition(_ => throw new Exception("error"))
        case "b" =>
          new Stage[String, String](id, next)((msg, ctx) =>
            Future{msg + "b"}
          )
      }
    }
    sd.process("hello") pipeTo self
    expectMsgType[Status.Failure]
  }

  "StageDriver" should "support ThreadLocals" in {
    val threadLocal = new ThreadLocal[Int]()
    threadLocal.set(10)
    MockThreadLocals.set(10)
    val sd = StageFactory[String, String] {
      "a" ~> "b"
    } createStageDriver {(id, next) =>
      val doWork: (String, ActorContext) => Future[String] = id match {
        case "a" => (msg, ctx) => {
          val p = Promise[String]()
          p.complete(Try {
            val num = threadLocal.get
            threadLocal.set(num * 10)
            msg + num
          })
          p.future
        }
        case "b" => (msg, ctx) => {
          val p = Promise[String]()
          p.complete(Try {
            msg + (threadLocal.get - 1)
          })
          p.future
        }
      }
      new Stage[String, String](id, next)(doWork).aroundReceive(_ => {
        threadLocal.set(MockThreadLocals.get.asInstanceOf[Int])
      }, _ => {
        MockThreadLocals.set(threadLocal.get)
        threadLocal.remove()
      })
    }
    sd.process("hello") pipeTo self
    expectMsg("hello1099")
  }
}

object MockThreadLocals {

  val defaultKey = "default"

  val map = TrieMap.empty[String, Any]

  def get = map.get(defaultKey) getOrElse null

  def set(value: Any) = map.put(defaultKey, value)
}