import akka.actor.Actor.Receive
import scala.concurrent.duration._
import scala.math._
import scala.util.Random
import akka.actor.Actor
import akka.actor.Props
import akka.actor._
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import scala.concurrent.ExecutionContext.Implicits.global
import akka.event.Logging
import akka.event.LoggingAdapter

case class Structurize(i: Int, j: Int, k: Int, nodes: Array[Array[Array[ActorRef]]], index: Int, cuberoot: Int, topology: String)
case class Gossip3D(selfFlag: Boolean)
case class GossipFull(NextIndex: Int, selfFlag: Boolean)

object GossipSim extends App {
  var allActors: Array[Int] = null;
  var ActorsAllFull: Array[ActorRef] = null;
  if (args.length == 0 || args.length != 3) {
    println("Arguments are not proper.")
  } else {
    var numNodes: Int = args(0).toInt
    val topology: String = args(1)
    val algorithm = args(2)
    var nodes_3d = Array.ofDim[ActorRef](0, 0, 0)
    val InfoSys = ActorSystem("InformationSystem")

    if ("3D".equalsIgnoreCase(topology) || "imperfect3D".equalsIgnoreCase(topology)) {
      //println(math.pow(numNodes, (0.33).toDouble).ceil.toInt);
      val cuberoot = math.pow(numNodes, (0.33).toDouble).ceil.toInt
      numNodes = cuberoot * cuberoot * cuberoot
      allActors = new Array[Int](numNodes)
      //Creating 3D nodes
      nodes_3d = Array.ofDim[ActorRef](cuberoot, cuberoot, cuberoot)
      var index: Int = 0

      for (i <- 0 to cuberoot - 1) {
        for (j <- 0 to cuberoot - 1) {
          for (k <- 0 to cuberoot - 1) {
            //println(index)
            nodes_3d(i)(j)(k) = InfoSys.actorOf(Props[Node])
          }
        }
      }
      //println(allActors.length)
      //println(index)

      for (i <- 0 to cuberoot - 1) {
        for (j <- 0 to cuberoot - 1) {
          for (k <- 0 to cuberoot - 1) {
            //println(index)
            nodes_3d(i)(j)(k) ! Structurize(i, j, k, nodes_3d, index, cuberoot, topology)
            allActors(index) = 1
            index += 1;
          }
        }
      }

      if (algorithm.equalsIgnoreCase("gossip")) {
        nodes_3d(Random.nextInt(cuberoot))(Random.nextInt(cuberoot))(Random.nextInt(cuberoot)) ! Gossip3D(false)
      }
    }

    if (topology.equalsIgnoreCase("line")) {

      //Creating nodes
      var index: Int = 0
      nodes_3d = Array.ofDim[ActorRef](1, 1, numNodes)
      allActors = new Array[Int](numNodes)
      for (k <- 0 to numNodes - 1) {
        nodes_3d(0)(0)(k) = InfoSys.actorOf(Props[Node])
        allActors(k)=1;
      }

      for (k <- 0 to numNodes - 1) {
        //Structurize(i: Int, j: Int, k: Int, nodes: Array[Array[Array[ActorRef]]], index: Int, cuberoot: Int, topology: String)
        nodes_3d(0)(0)(k) ! Structurize(0, 0, k, nodes_3d, index, numNodes, topology)
        index = index + 1
      }
      
      if (algorithm.equalsIgnoreCase("gossip")) {
        nodes_3d(0)(0)(Random.nextInt(numNodes)) ! Gossip3D(false)
      }
    }

//    if (topology.equalsIgnoreCase("3D")) {
//
//    }

    if (topology.equalsIgnoreCase("full")) {
      ActorsAllFull = new Array[ActorRef](numNodes)
      allActors = new Array[Int](numNodes)
      for (i <- 0 to numNodes - 1) {
        ActorsAllFull(i) = InfoSys.actorOf(Props[Node])
        allActors(i) = 1;
      }
      if (algorithm.equalsIgnoreCase("gossip")) {
        var randomfullnext: Int = Random.nextInt(ActorsAllFull.length);
        ActorsAllFull(randomfullnext) ! GossipFull(randomfullnext, false)
      }
    }
  }
}

class Node() extends Actor {

  var thisNode: ActorRef = null
  var neighbor = Array.ofDim[ActorRef](7)
  var thisIndex: Int = 0
  var GossipCount: Int = 0
  var neighbourijk = Array.ofDim[String](7)
  var ijk_index: Int = 0
  var size: Int = 0;
  var neighbor_index: Int = 0
  //context.system.eventStream.subscribe(self, classOf[DeadLetter])
  def receive = {
    case Structurize(i: Int, j: Int, k: Int, nodes: Array[Array[Array[ActorRef]]], index: Int, cuberoot: Int, topology: String) => {
      thisIndex = index;
      thisNode = nodes(i)(j)(k)
      //println("Init "+NodeIndex)    
      size = cuberoot;

      if (!topology.equalsIgnoreCase("line")) {
        //left
        if (i != 0) {
          neighbor(neighbor_index) = nodes(i - 1)(j)(k)
          neighbor_index += 1
          neighbourijk(ijk_index) = (i - 1).toString() + "|" + j.toString() + "|" + k.toString()
          ijk_index += 1
        }
        //right
        if (i != cuberoot - 1) {
          neighbor(neighbor_index) = nodes(i + 1)(j)(k)
          neighbor_index += 1
          neighbourijk(ijk_index) = (i + 1).toString() + "|" + j.toString() + "|" + k.toString()
          ijk_index += 1
        }
        //back
        if (j != 0) {
          neighbor(neighbor_index) = nodes(i)(j - 1)(k)
          neighbor_index += 1
          neighbourijk(ijk_index) = i.toString() + "|" + (j - 1).toString() + "|" + k.toString()
          ijk_index += 1
        }
        //front
        if (j != cuberoot - 1) {
          neighbor(neighbor_index) = nodes(i)(j + 1)(k)
          neighbor_index += 1
          neighbourijk(ijk_index) = i.toString() + "|" + (j + 1).toString() + "|" + k.toString()
          ijk_index += 1
        }
      }
      //bottom
      if (k != 0) {
        neighbor(neighbor_index) = nodes(i)(j)(k - 1)
        neighbor_index += 1
        neighbourijk(ijk_index) = i.toString() + "|" + j.toString() + "|" + (k - 1).toString()
        ijk_index += 1
      }
      //top
      if (k != cuberoot - 1) {
        neighbor(neighbor_index) = nodes(i)(j)(k + 1)
        neighbor_index += 1
        neighbourijk(ijk_index) = i.toString() + "|" + j.toString() + "|" + (k + 1).toString()
        ijk_index += 1
      }

      //      print(index+" >> ")
      //      for(neighbor_index <- 0 to neighbor.length-1) {print("%s , ".format(neighbor(neighbor_index)))}
      //      println()

      if (topology.equalsIgnoreCase("imperfect3d")) {
        var randomi: Int = Random.nextInt(cuberoot);
        var randomj: Int = Random.nextInt(cuberoot);
        var randomk: Int = Random.nextInt(cuberoot);
        var randomNeigh: ActorRef = nodes(randomi)(randomj)(randomk);
        while ((randomNeigh.equals(self)) || (neighbor contains randomNeigh)) {
          randomi = Random.nextInt(cuberoot);
          randomj = Random.nextInt(cuberoot);
          randomk = Random.nextInt(cuberoot);
          randomNeigh = nodes(randomi)(randomj)(randomk);
        }
        neighbor(neighbor_index) = randomNeigh
        neighbor_index += 1
        neighbourijk(ijk_index) = randomi.toString() + "|" + randomj.toString() + "|" + randomk.toString()
        ijk_index += 1
      }
    }
    //case DeadLetter(msg,from,to)=>{print("")}
    case Gossip3D(selfFlag: Boolean) => {

      var Randijk: String = null;
      var RandNo: Int = 0;
      var RandomNeighbor: ActorRef = null;
      var Neighbori: Int = 0;
      var Neighborj: Int = 0;
      var Neighbork: Int = 0;
      var OrigIndex: Int = 0;
      var OrigIndex_split: Array[String] = null;
      var shutdown: Boolean = false;
      if (GossipCount < 10) {
        if (!selfFlag) {
          GossipCount += 1;
          println("Gossip Count of Actor " + thisIndex + " reached " + GossipCount + " !")
        }
        RandNo = Random.nextInt(neighbor_index)
        //while(neighbor(RandNo).isTerminated){RandNo = Random.nextInt(neighbor_index)}
        //      println("Actor "+thisIndex+" 1 "+RandNo)
        Randijk = neighbourijk(RandNo)
        //      println("Actor "+thisIndex+" 2 "+neighbor.length)
        //      println("Actor "+thisIndex+" 2 "+neighbourijk.length)
        //      println("Actor "+thisIndex+" 2 "+Randijk)

        //      println("Actor "+thisIndex+" 2 test"+neighbourijk(0))
        //      println("Actor "+thisIndex+" 2 test"+neighbourijk(1))
        //      println("Actor "+thisIndex+" 2 test"+neighbourijk(2))
        //      println("Actor "+thisIndex+" 2 test"+neighbourijk(3))
        //      println("Actor "+thisIndex+" 2 test"+neighbourijk(4))
        //      println("Actor "+thisIndex+" 2 test"+neighbourijk(5))      
        //      OrigIndex_split=Randijk.split("|");
        //      for(z<-0 to OrigIndex_split.length-1){println("Actor "+thisIndex+" 3 "+OrigIndex_split(z))}
        Neighbori = Randijk.split("|")(0).toInt
        //      println("Actor "+thisIndex+" 4 "+Neighbori)
        Neighborj = Randijk.split("|")(2).toInt
        //      println("Actor "+thisIndex+" 5 "+Neighborj)
        Neighbork = Randijk.split("|")(4).toInt
        //      println("Actor "+thisIndex+" 6 "+Neighbork)
        OrigIndex = (((Neighbori * size) + Neighborj) * size) + Neighbork
        if (GossipSim.allActors(OrigIndex) == 1) {
          RandomNeighbor = neighbor(RandNo)
          println("Gossip sent from " + thisIndex + " to " + OrigIndex)
          //if(GossipSim.allActors(OrigIndex) == 1){
          RandomNeighbor ! Gossip3D(false)
          //}
        }
      }
      if (GossipCount < 9) { self ! Gossip3D(true) }
      if (GossipCount == 10) {
        println("Gossip Count of Actor " + thisIndex + " maxed out.Shutting down node");
        GossipSim.allActors(thisIndex) = 0
        //self ! PoisonPill
        context.stop(self)
      }
      //var scheduler = context.system.scheduler.schedule(FiniteDuration(0,MILLISECONDS), FiniteDuration(50,MILLISECONDS),self,Gossip)

    }
    case GossipFull(nextIndex: Int, selfFlag: Boolean) => {
      thisIndex = nextIndex
      var randomnode: Int = Random.nextInt(GossipSim.ActorsAllFull.length)
      var Next: ActorRef = GossipSim.ActorsAllFull(randomnode)
      while (Next.equals(self)) {
        randomnode = Random.nextInt(GossipSim.ActorsAllFull.length);
        Next = GossipSim.ActorsAllFull(randomnode)
        if (GossipSim.allActors(randomnode) == 0) {
          Next = self
        }
      }

      if (GossipCount < 10) {
        if (GossipSim.allActors(randomnode) == 1) {
          if (!selfFlag) {
            GossipCount += 1;
            println("Gossip Count of Actor " + thisIndex + " reached " + GossipCount + " !")
          }
          println("Gossip sent from " + thisIndex + " to " + randomnode)
          Next ! GossipFull(randomnode, false)
        }
      }
      if (GossipCount < 9) {
        self ! GossipFull(thisIndex, true)
      }
      if (GossipCount == 10) {
        println("Gossip Count of Actor " + thisIndex + " maxed out.Shutting down node");
        GossipSim.allActors(thisIndex) = 0
        context.stop(self)
      }
    }
  }
}