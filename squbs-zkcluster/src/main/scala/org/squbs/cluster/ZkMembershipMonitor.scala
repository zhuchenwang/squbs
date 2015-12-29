package org.squbs.cluster

import java.util.concurrent.atomic.AtomicBoolean

import akka.actor.{Actor, Address, AddressFromURIString}
import com.typesafe.scalalogging.LazyLogging
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.api.CuratorWatcher
import org.apache.curator.framework.recipes.leader.{LeaderLatch, LeaderLatchListener}
import org.apache.zookeeper.Watcher.Event.EventType
import org.apache.zookeeper.{CreateMode, WatchedEvent}

import scala.collection.JavaConversions._

/**
 * Created by zhuwang on 1/26/15.
 */

private[cluster] case class ZkLeaderElected(address: Option[Address])
private[cluster] case class ZkMembersChanged(members: Set[Address])

/**
 * the membership monitor has a few responsibilities, 
 * most importantly to enroll the leadership competition and get membership,
 * leadership information immediately after change
 */
private[cluster] class ZkMembershipMonitor extends Actor with LazyLogging {

  private[this] val zkCluster = ZkCluster(context.system)
  import zkCluster._

  private[this] implicit val log = logger
  private[this] var zkLeaderLatch: LeaderLatch = null
  private[this] val stopped = new AtomicBoolean(false)
  
  def initialize()(implicit zkClient: CuratorFramework) = {
    //watch over leader changes
    zkClient.getData.usingWatcher(new CuratorWatcher {
      override def process(event: WatchedEvent): Unit = {
        log.debug("[membership] leader watch event:{} when stopped:{}", event, stopped.toString)
        if(!stopped.get && event.getType == EventType.NodeDataChanged) {
          zkClusterActor ! ZkLeaderElected(zkClient.getData.usingWatcher(this).forPath("/leader"))
        }
      }
    }).forPath("/leader")

    //watch over my self
    val me = guarantee(s"/members/${keyToPath(zkAddress.toString)}", Some(Array[Byte]()), CreateMode.EPHEMERAL)
    // Watch and recreate member node because it's possible for ephemeral node to be deleted while session is
    // still alive (https://issues.apache.org/jira/browse/ZOOKEEPER-1740)
    zkClient.getData.usingWatcher(new CuratorWatcher {
      def process(event: WatchedEvent): Unit = {
        log.debug("[membership] self watch event: {} when stopped:{}", event, stopped.toString)
        if(!stopped.get && event.getType == EventType.NodeDeleted) {
          log.info("[membership] member node was deleted unexpectedly, recreate")
          zkClient.getData.usingWatcher(this).forPath(guarantee(me, Some(Array[Byte]()), CreateMode.EPHEMERAL))
        }
      }
    }).forPath(me)

    //watch over members changes
    lazy val members = zkClient.getChildren.usingWatcher(new CuratorWatcher {
      override def process(event: WatchedEvent): Unit = {
        log.debug("[membership] membership watch event:{} when stopped:{}", event, stopped.toString)
        if(!stopped.get && event.getType == EventType.NodeChildrenChanged) {
          refresh(zkClient.getChildren.usingWatcher(this).forPath("/members"))
        }
      }
    }).forPath("/members")

    def refresh(members: Seq[String]) = {
      // tell the zkClusterActor to update the memory snapshot
      zkClusterActor ! ZkMembersChanged(members.map(m => AddressFromURIString(pathToKey(m))).toSet)
    }

    refresh(members)
  }
  
  override def postStop = {
    //stop the leader latch to quit the competition
    stopped set true
    if (zkLeaderLatch != null) zkLeaderLatch.close
  }
  
  def receive: Actor.Receive = {
    case ZkClientUpdated(updated) =>
      // differentiate first connected to ZK or reconnect after connection lost
      implicit val zkClient = updated
      if (zkLeaderLatch != null) zkLeaderLatch.close
      zkLeaderLatch = new LeaderLatch(zkClient, "/leadership")
      initialize()
      zkLeaderLatch.addListener(new LeaderLatchListener {
        override def isLeader: Unit = {
          log.info("[membership] leadership acquired @ {}", zkAddress)
          guarantee("/leader", Some(zkAddress))(zkClient, log)
        }
        override def notLeader(): Unit = {
          zkClusterActor ! ZkLeaderElected(zkClient.getData.forPath("/leader"))
        }
      }, context.dispatcher)
      zkLeaderLatch.start
      zkClusterActor ! ZkLeaderElected(zkClient.getData.forPath("/leader"))
  }
}