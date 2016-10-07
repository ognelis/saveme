import java.io.{ByteArrayInputStream, ObjectInputStream, ObjectOutputStream}

import scala.collection.concurrent.TrieMap

/**
  * Created by revenskiy_ag on 05.10.16.
  */

trait Agent extends Serializable {
  val address: String
  val port: String
  val id: String
  var masterAgents: scala.collection.concurrent.TrieMap[String, Agent] = new TrieMap()

  def toByteArray = {
    val bytes = new java.io.ByteArrayOutputStream()
    val oos   = new ObjectOutputStream(bytes)
    oos.writeObject(this); oos.close()
    bytes.toByteArray
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Agent]

  override def hashCode(): Int = {41*(41*((41 + address.hashCode) + port.hashCode) + id.hashCode)}
  override def equals(other: scala.Any): Boolean = other match {
    case that: Agent => {
      (that canEqual this) &&
      address == that.address &&
      port == that.port &&
      id == that.id
    }
    case _ => false
  }
  override def toString: String = s"$address:$port{$id}"
}

object Agent {
  def apply(addressNew:String,portNew:String,idNew:String) = new Agent{
    val address = addressNew
    val port = portNew
    val id   = idNew
  }
  def serialize(bytes: Array[Byte]) = {
    val bytesOfObject = new ObjectInputStream(new ByteArrayInputStream(bytes))
    bytesOfObject.readObject().asInstanceOf[Agent]
  }
}