package org.squbs.pipeline.util

import org.scalatest.{FlatSpecLike, Matchers}

import scala.collection.immutable.ListMap

/**
 * Created by zhuwang on 8/22/14.
 */
class StageTransitionUtilTest extends FlatSpecLike with Matchers {

  "StageTransitionUtil" should "turn single id to ListMap" in {
    import StageTransitionUtil._
    val map: ListMap[String, Option[String]] = "hello"
    map.size should be (1)
    map.get("hello") should be (Some(None))
  }

  "StageTransitionUtil" should "chain all the ids" in {
    import StageTransitionUtil._
    val map: ListMap[String, Option[String]] = {
      "a" ~> "b" ~> "c" ~> "d" ++ "f" ~> "g"
    }
    map.size should be (6)
    map.get("b") should be (Some(Some("c")))
    map.get("f") should be (Some(Some("g")))
  }
}
