package org.squbs.cluster

import java.util.concurrent.atomic.AtomicBoolean

import akka.actor._
import akka.util.ByteString
import com.typesafe.scalalogging.LazyLogging
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.api.CuratorWatcher
import org.apache.zookeeper.Watcher.Event.EventType
import org.apache.zookeeper.{CreateMode, WatchedEvent}

import scala.collection.JavaConversions._
import scala.util.Try

/**
 * Created by zhuwang on 1/26/15.
 */

private[cluster] case class ZkRebalance(planedPartitions: Map[ByteString, ZkPartitionData])
private[cluster] case class ZkPartitionsChanged(segment:String, partitions: Map[ByteString, ZkPartitionData])
private[cluster] case class ZkResizePartition(partitionKey: ByteString, size: Int)
private[cluster] case class ZkSegmentChanged(segment: String, changes: Set[ByteString])
private[cluster] case class ZkOnBoardPartitions(onBoards: Set[ByteString])
private[cluster] case class ZkDropOffPartitions(dropOffs: Set[ByteString])
private[cluster] case object ZkGetPartitions

/**
 * The major responsibility of ZkPartitionsManager is to maintain partitions
 */
private[cluster] class ZkPartitionsManager extends Actor with Stash with LazyLogging {
  private[this] val zkCluster = ZkCluster(context.system)
  import zkCluster._
  
  private[this] implicit val segLogic = segmentationLogic
  import segLogic._
  
  private[this] implicit val log = logger
  implicit val ec = context.dispatcher
//  private[cluster] var partitionsToProtect = Set.empty[ByteString]
  private[this] var segmentsToPartitions = Map.empty[String, Set[ByteString]]
  private[this] var partitionWatchers = Map.empty[String, CuratorWatcher]
  private[this] val stopped = new AtomicBoolean(false)
  
  def initialize()(implicit zkClient: CuratorFramework) = {
    segmentsToPartitions = zkClient.getChildren.forPath("/segments").map{
      segment => segment -> watchOverSegment(segment)
    }.toMap
  }

  override def postStop = stopped set true
  
  def watchOverSegment(segment:String)(implicit zkClient: CuratorFramework) = {
    val segmentZkPath = s"/segments/${keyToPath(segment)}"
    //watch over changes of creation/removal of any partition (watcher over /partitions)
    lazy val segmentWatcher: CuratorWatcher = new CuratorWatcher {
      override def process(event: WatchedEvent): Unit = {
        event.getType match {
          case EventType.NodeChildrenChanged if !stopped.get =>
            self ! ZkSegmentChanged(
              segment,
              zkClient.getChildren.usingWatcher(segmentWatcher).forPath(segmentZkPath)
                .map { p => ByteString(pathToKey(p))}.toSet
            )
          case _ =>
        }
      }
    }
    //watch over changes of members of a partition (watcher over /partitions/some-partition)
    lazy val partitionWatcher: CuratorWatcher = new CuratorWatcher {
      override def process(event: WatchedEvent): Unit = {
        event.getType match {
          case EventType.NodeDataChanged if !stopped.get =>
            val sectors = event.getPath.split("[/]")
            val partitionKey = ByteString(pathToKey(sectors(sectors.length - 2)))
            sectors(sectors.length - 1) match {
              case "servants" | "$size" =>
                watchOverPartition(segment, partitionKey, this) foreach {partitionData =>
                  whenPartitionChanged(segment, partitionData)
                }
              case _ =>
            }
          case _ =>
        }
      }
    }
    partitionWatchers += segment -> partitionWatcher
    //initialize with the current set of partitions
    zkClient.getChildren.usingWatcher(segmentWatcher).forPath(segmentZkPath).map{p =>
      val partitionKey = ByteString(pathToKey(p))
      partitionKey -> watchOverPartition(segment, partitionKey, partitionWatcher)
    }.collect{
      case (partitionKey, Some(partitionData)) => partitionKey
    }.toSet
  }
  
  private def whenPartitionChanged(segment: String, change: ZkPartitionData) = {
    log.debug("[partitions] partitions change detected from zk: {}",
      keyToPath(change.partitionKey) -> change
    )
    zkClusterActor ! ZkPartitionsChanged(segment, Map(change.partitionKey -> change))
  }
  
  def watchOverPartition(segment: String,
                         partitionKey: ByteString,
                         partitionWatcher: CuratorWatcher)
                        (implicit zkClient: CuratorFramework): Option[ZkPartitionData] = {
    try {
      guarantee(servantsOfParZkPath(partitionKey), None, CreateMode.PERSISTENT)
      guarantee(sizeOfParZkPath(partitionKey), None, CreateMode.PERSISTENT)
      val servants: Set[Address] =
        zkClient.getData.usingWatcher(partitionWatcher).forPath(servantsOfParZkPath(partitionKey))
      val expectedSize: Int =
        zkClient.getData.usingWatcher(partitionWatcher).forPath(sizeOfParZkPath(partitionKey))
      Some(ZkPartitionData(partitionKey, servants, partitionSize(partitionKey), expectedSize))
    }
    catch {
      case t: Throwable => log.error("partitions refresh failed due to unknown reason: {}", t.getMessage)
        None
    }
  }
  
  def receive: Receive = {
    
    case ZkClientUpdated(updated) =>
      implicit val zkClient = updated
      initialize()
      context.become(receivePartitionChange(), true)
  }

  def receivePartitionChange()(implicit zkClient: CuratorFramework): Receive = {
    case ZkClientUpdated(updated) =>
      implicit val zkClient = updated
      initialize()
      context.become(receivePartitionChange(), true)
    case ZkSegmentChanged(segment, changes) =>
      log.debug("[partitions] segment change detected from zk: {}", segment -> (changes map (keyToPath(_))))
      val onBoardPartitions = changes.diff(segmentsToPartitions.getOrElse(segment, Set.empty))
        .map(partitionKey => (partitionKey -> watchOverPartition(segment, partitionKey, partitionWatchers(segment))))
        .collect{case (key, Some(partition)) => key -> partition}.toMap
      val dropOffPartitions = segmentsToPartitions.getOrElse(segment, Set.empty) diff changes
      segmentsToPartitions += (segment -> changes)
      log.info("[partitions] create partitions {}, remove partitions {}",
        onBoardPartitions.map(entry => keyToPath(entry._1)),
        dropOffPartitions.map(entry => keyToPath(entry))
      )
      if (onBoardPartitions.nonEmpty)
        zkClusterActor ! ZkPartitionsChanged(segment, onBoardPartitions)
      if (dropOffPartitions.nonEmpty)
        zkClusterActor ! ZkPartitionsChanged(segment,
          dropOffPartitions.map(key => key -> ZkPartitionData(key, expectedSize = 0)).toMap
        )

    case ZkRebalance(updates) =>
      log.info("[partitions] update partitions based on plan:{}",
        updates.map{case (key, value) => keyToPath(key) -> (value.members, value.expectedSize)}
      )
      updates foreach {
        case (partitionKey, partitionData) =>
          guarantee(partitionZkPath(partitionKey), Some(partitionData.props), CreateMode.PERSISTENT)
          guarantee(servantsOfParZkPath(partitionKey), Some(partitionData.members), CreateMode.PERSISTENT)
          if (partitionData.expectedSize != partitionSize(partitionKey))
            guarantee(sizeOfParZkPath(partitionKey), Some(partitionData.expectedSize), CreateMode.PERSISTENT)
      }

    case ZkRemovePartition(partitionKey) =>
      log.debug("[partitions] remove partition {}", keyToPath(partitionKey))
      safelyDiscard(partitionZkPath(partitionKey))
      sender ! ZkPartitionRemoval(partitionKey)

    case ZkResizePartition(partitionKey, size) =>
      guarantee(sizeOfParZkPath(partitionKey), Some(size), CreateMode.PERSISTENT)

    case ZkGetPartitions =>
      sender() ! loadPartitions()
  }

  def loadPartitions()(implicit zkClient: CuratorFramework): Map[ByteString, ZkPartitionData] = {
    zkClient.getChildren.forPath("/segments") flatMap { segment =>
      zkClient.getChildren.forPath(s"/segments/$segment")
    } map { key =>
      val parKey = ByteString(pathToKey(key))
      val size = partitionSize(parKey)
      val members = partitionServants(parKey)
      val props = Try(zkClient.getData.forPath(partitionZkPath(parKey))) getOrElse Array.empty
      parKey -> ZkPartitionData(parKey, members, size, props)
    } toMap
  }

  private def partitionServants(partitionKey: ByteString)(implicit zkClient: CuratorFramework): Set[Address] = {
    try{
      zkClient.getData.forPath(servantsOfParZkPath(partitionKey))
    }
    catch{
      case _:Throwable => Set.empty
    }
  }

  private def partitionSize(partitionKey: ByteString)(implicit zkClient: CuratorFramework): Int = {
    try{
      zkClient.getData.forPath(sizeOfParZkPath(partitionKey))
    }
    catch{
      case _:Throwable => 0
    }
  }
}
