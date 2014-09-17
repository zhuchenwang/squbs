package org.squbs.pipeline.util

import scala.collection.immutable.ListMap

/**
 * Created by zhuwang on 8/22/14.
 */
object StageTransitionUtil {

  implicit def toTransitionMap[ID](id: ID): ListMap[ID, Option[ID]] = {
    ListMap(id -> None)
  }

  implicit class SingleTransition[ID](id: ID) {

    def ~>(next: ID): ListMap[ID, Option[ID]] = ListMap(
      id -> Some(next),
      next -> None
    )
  }

  implicit class ChainedTransition[ID](transitions: ListMap[ID, Option[ID]]) {

    def ~>(next: ID): ListMap[ID, Option[ID]] = {
      if (transitions.get(next).nonEmpty) throw new Exception(s"Circular transition on ${next}")
      val last = transitions.find(_._2 == None)
      val updatedMap = transitions + (last.get._1 -> Some(next))
      updatedMap + (next -> None)
    }
  }
}
