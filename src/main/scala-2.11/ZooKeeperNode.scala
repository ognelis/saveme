import java.util

import com.twitter.common.zookeeper.DistributedLockImpl
import org.apache.zookeeper.AsyncCallback.ChildrenCallback
import org.apache.zookeeper.{CreateMode, WatchedEvent, Watcher, ZooDefs}
import org.apache.zookeeper.data.Stat

import scala.collection.concurrent.TrieMap
import scala.collection.mutable

/**
  * Created by revenskiy_ag on 10.10.16.
  */


abstract class ZooKeeperNode(zoo: ZooKeeper, val mode: CreateMode) {
  val parentPath: String
  val name: String

  protected def getNodeMetaInformation: Option[Stat] = Option(zoo.zooKeeper.exists(s"$parentPath/$name",false))
  protected def idOfNode(path: String) = path.substring(path.lastIndexOf("/") + 1)

  def setData(data :Data): Boolean = getNodeMetaInformation match {
    case Some(stat) => zoo.zooKeeper.setData(s"$parentPath/$name", data.toByteArray, stat.getVersion); true
    case None => false
  }
  def getData: Data = getNodeMetaInformation match {
      case Some(stat) => {
        val bytes:Array[Byte] = zoo.zooKeeper.getData(s"$parentPath/$name", false, stat)
        if (bytes.length>1) Data.serialize(bytes) else NoData()
      }
      case None => throw new NoSuchElementException(s"Node: $parentPath/$name doesn't exist in Zookeeper!")
  }
  def create(): String
  def remove(): Unit = getNodeMetaInformation match {
      case Some(stat) => zoo.zooKeeper.delete(s"$parentPath/$name",stat.getVersion)
      case None => throw new NoSuchElementException(s"This Node: $parentPath/$name doesn't exist in Zookeeper!")
  }
  def bindTo(that: ZooKeeperNode): Boolean = that.setData(getData)
  override def toString: String = s"$parentPath/$name"
}


case class PathNode(zoo: ZooKeeper, override val name: String, parentPath: String)
  extends ZooKeeperNode(zoo, CreateMode.PERSISTENT)
{
  val children: mutable.ArrayBuffer[ZooKeeperNode] = mutable.ArrayBuffer[ZooKeeperNode]()
  override def create(): String =
    zoo.zooKeeper.create(s"$parentPath/$name", Array[Byte](0), ZooDefs.Ids.OPEN_ACL_UNSAFE, mode)
}


//side effect
case class MasterNode(zoo: ZooKeeper, parentPath: String)
  extends ZooKeeperNode(zoo, CreateMode.PERSISTENT_SEQUENTIAL)
{
  override val name = idOfNode(zoo.zooKeeper.create(s"$parentPath/", Array[Byte](0), ZooDefs.Ids.OPEN_ACL_UNSAFE, mode))
  override def create(): String = name
}



case class ParticipantNode(zoo: ZooKeeper, parentPath: String)
  extends ZooKeeperNode(zoo, CreateMode.PERSISTENT_SEQUENTIAL)
{
  private val children: mutable.ArrayBuffer[DataNode] = mutable.ArrayBuffer[DataNode]()
  def addChild(data: DataNode) = {
    zoo.zooKeeper.getChildren(s"$parentPath/$name",myWatcher,callbackChildren,None)
    addChildAndCreate(data)
  }

  private val myWatcher:Watcher = new Watcher() {override def process(event: WatchedEvent): Unit = {}}
  private var childrenCallback: mutable.Buffer[String] = new mutable.ArrayBuffer[String]()
  private def callbackChildren = new ChildrenCallback {
    override def processResult(rc: Int, path: String, ctx: scala.Any, children: util.List[String]): Unit = {
      import collection.JavaConverters._
      childrenCallback = children.asScala
    }
  }

  def addChildAndCreate(data: DataNode) = {children += data; data.create()}
  override val name = idOfNode(zoo.zooKeeper.create(s"$parentPath/", Array[Byte](0), ZooDefs.Ids.OPEN_ACL_UNSAFE, mode))
  override def create(): String = name
}



case class DataNode(zoo: ZooKeeper, parentPath: String, data: Data)
  extends ZooKeeperNode(zoo, CreateMode.EPHEMERAL)
{
  val masterAgents: scala.collection.concurrent.TrieMap[String, Agent] = new TrieMap()

  def lockAndOpen(masterNode: MasterNode) = {
    val lockedMaster = new DistributedLockImpl(zoo.zooKeeperClient, s"${masterNode.toString}")
    val master = Agent.serialize(masterNode.getData.toByteArray)
    lockedMaster.tryLock(100, java.util.concurrent.TimeUnit.MILLISECONDS)
    if (masterAgents.isDefinedAt(masterNode.toString)) {
      if (masterAgents(masterNode.toString) == master) {
        chooseMaster(); ???} else masterAgents(masterNode.toString) = master
    } else {
      masterAgents += ((masterNode.toString,master))
    }
    lockedMaster.unlock()
  }

  def chooseMaster() = ???

  override val name: String = data.toString
  override def create(): String = {
    idOfNode(zoo.zooKeeper.create(s"$parentPath/$name", data.toByteArray, ZooDefs.Ids.OPEN_ACL_UNSAFE, mode))
  }
}


case class Root(zoo: ZooKeeper, participantPathName: String, masterPathName: String)
{
  val parent = ""
  val masterPath: PathNode = PathNode(zoo, masterPathName, parent)
  val participantPath: PathNode = PathNode(zoo, participantPathName, parent)
}