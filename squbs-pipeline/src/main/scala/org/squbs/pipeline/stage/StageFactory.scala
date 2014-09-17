package org.squbs.pipeline.stage

import scala.collection.immutable.ListMap
import scala.concurrent.{Promise, Future}

/**
 * Created by zhuwang on 8/22/14.
 */
class StageFactory[ID, CTX](transitionMap: ListMap[ID, Option[ID]]) {

  require(transitionMap.nonEmpty, "The transition map cannot be empty")

  def createStageDriver(create: (ID, Option[ID]) => Stage[ID, CTX]): StageDriver[ID, CTX] = {
    val stages = transitionMap map {
      case (id, next) => id -> create(id, next)
    }
    new StageDriver(stages.head._2, stages - stages.head._1)
  }
}

object StageFactory {

  def apply[ID, CTX](fn: => ListMap[ID, Option[ID]]) = {
    new StageFactory[ID, CTX](fn)
  }
}

class StageDriver[ID, CTX](firstStage: Stage[ID, CTX], stagesMap: Map[ID, Stage[ID, CTX]]) {

  def process(input: CTX): Future[CTX] = {
    val p = Promise[CTX]
    firstStage.process(input, p, stagesMap)
    p.future
  }
}