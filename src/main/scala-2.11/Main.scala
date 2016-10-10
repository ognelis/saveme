
import java.net.{InetAddress, InetSocketAddress}

import com.twitter.common.zookeeper.{ZooKeeperClient, ZooKeeperNode, ZooKeeperUtils}
import org.apache.zookeeper.{CreateMode, ZooDefs}
import org.apache.zookeeper


object Main extends App {

  val ipAddress = InetAddress.getByName("172.17.0.2")
  val port = 2181

  val zooKeeper = new ZooKeeper(ipAddress, port)

 // zooKeeper.zooKeeper.getChildren("/",false)

  val root = Root(zooKeeper, "participant","master")

  val master1 = MasterNode(zooKeeper,root.masterPath.toString)
  val master2 = MasterNode(zooKeeper,root.masterPath.toString)
  val master3 = MasterNode(zooKeeper,root.masterPath.toString)
  val master4 = MasterNode(zooKeeper,root.masterPath.toString)
  val master5 = MasterNode(zooKeeper,root.masterPath.toString)
  val master6 = MasterNode(zooKeeper,root.masterPath.toString)
  root.masterPath.children += master1
  root.masterPath.children += master2
  root.masterPath.children += master3
  root.masterPath.children += master4
  root.masterPath.children += master5
  root.masterPath.children += master6

  val participant = ParticipantNode(zooKeeper,root.participantPath.toString)
  root.participantPath.children += participant

  participant.setData(Partition(master1.toString))



  val agent1 = Agent("192.168.0.1","2222","1"); val dataNode1 = DataNode(zooKeeper,participant.toString, agent1)
  val agent2 = Agent("192.168.0.2","2222","1"); val dataNode2 = DataNode(zooKeeper,participant.toString, agent2)
  val agent3 = Agent("192.168.0.3","2222","1"); val dataNode3 = DataNode(zooKeeper,participant.toString, agent3)
  val agent4 = Agent("192.168.0.4","2222","1"); val dataNode4 = DataNode(zooKeeper,participant.toString, agent4)
  val agent5 = Agent("192.168.0.5","2222","1"); val dataNode5 = DataNode(zooKeeper,participant.toString, agent5)
  val agent6 = Agent("192.168.0.6","2222","1"); val dataNode6 = DataNode(zooKeeper,participant.toString, agent6)

  participant.addChildAndCreate(dataNode1)
  participant.addChildAndCreate(dataNode2)
  participant.addChildAndCreate(dataNode3)
  participant.addChildAndCreate(dataNode4)
  participant.addChildAndCreate(dataNode5)
  participant.addChildAndCreate(dataNode6)


  println(participant.children)
  println(root.participantPath.children)


//  val stream = new Stream(zooKeeper,"participant","master")
////    stream.configure()
//
//  val agent = Agent("192.168.0.1","2222","1")
//  val agentAnother = Agent("172.16.0.1","2121","3")
//
//
//  val  (masterId,partcipantId) = stream.createMasterIdAndPaticipantIdAndBindThem(Some(agent))
//  stream.addAgentToParticipant(partcipantId,Agent("192.168.0.2","2552","2"))
//  stream.addAgentToParticipant(partcipantId,Agent("192.168.0.3","2662","3"))
//  stream.addAgentToParticipant(partcipantId,Agent("192.168.0.4","2662","4"))
//  stream.addAgentToParticipant(partcipantId,Agent("192.168.0.5","2662","5"))
//  stream.addAgentToParticipant(partcipantId,Agent("192.168.0.6","2662","6"))
//  stream.addAgentToParticipant(partcipantId,Agent("192.168.0.7","2662","7"))
//
// // val master = stream.getMasterData()
//  println(agent.masterAgents)





//  stream.createMasterIdAndPaticipantIdAndBindThem(Some(agent))
//  stream.createMasterIdAndPaticipantIdAndBindThem(Some(agent))
//  stream.createMasterIdAndPaticipantIdAndBindThem(Some(agent))
//  stream.createMasterIdAndPaticipantIdAndBindThem(Some(agent))


}
