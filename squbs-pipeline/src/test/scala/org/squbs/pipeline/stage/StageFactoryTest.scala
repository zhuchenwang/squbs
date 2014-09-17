package org.squbs.pipeline.stage

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.{FlatSpecLike, Matchers}

import scala.concurrent.{ExecutionContext, Future}
import akka.pattern._

/**
 * Created by zhuwang on 8/22/14.
 */
class StageFactoryTest extends TestKit(ActorSystem("StageFactoryTest"))
  with FlatSpecLike with Matchers with ImplicitSender {

  import org.squbs.pipeline.util.StageTransitionUtil._

  implicit val executionContext: ExecutionContext = system.dispatcher

  "StageFactory" should "create StageDriver according to the input" in {
    val stageDriver = StageFactory[String, String] {
      "a" ~> "b"
    } createStageDriver {(id, next) =>
      id match {
        case "a" => new Stage[String, String](id, next)((msg, ctx) =>
          Future{msg + "a"}
        )
        case "b" => new Stage[String, String](id, next)((msg, ctx) =>
          Future{msg + "b"}
        )
      }
    }
    stageDriver.process("hello ") pipeTo self
    expectMsg("hello ab")
  }

}
