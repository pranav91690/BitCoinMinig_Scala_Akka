import java.security.MessageDigest

import akka.actor.{ActorSystem, Props, ActorRef, Actor}
import akka.routing.RoundRobinRouter
import scala.collection.mutable.ListBuffer
import scala.language.postfixOps

/**
 * Created by pranav on 9/10/15.
 */
object project1{
  // Message to be send to the Master
  case object StartMining
  // Message sent by the Master to the Worker
  case class Work(start : Int, noOfElements : Int, noOfZeroes : Int)
  // Message sent by the Worker back to the Master
  case class Result(hashList : List[String])
  // Message sent by the Master to the Listener
  case class aggregateHash(duration : Long)
  // Message Sent by Worker to Indicate he's ready for work
  case object giveWork
  // Message Sent to Worker to Ask for Work
  case class getWork(ip : String)

  // The Worker Actor
  class Worker extends Actor {
    // To Calculate the SHA256 Hashes
    def SHA256(start : Int, noOfElements : Int, noOfZeroes : Int) : List[String] = {
      val pattern = "0{" + noOfZeroes + ",}[A-Fa-f0-9]+"
      val end = start + noOfElements
      var result = ListBuffer[String]()

      var count = start
      while(count <= end){
        val hashMe = "cshilpa12  + " + count
        // New SHA256 Algo
        val m = MessageDigest.getInstance("SHA-256").digest(hashMe.getBytes("UTF-8"))
        val sha256 = m.map("%02x".format(_)).mkString
        if(sha256.matches(pattern)){
          result += (hashMe + " : " + sha256)
        }
        count += 1
      }
//      result.foreach(println(_))
      result.toList
    }

    // The Receive Method
    def receive = {
      case Work(start, noOfElements, noOfZeroes) =>
        sender ! Result(SHA256(start, noOfElements, noOfZeroes))
        sender ! giveWork

      case getWork(ip) =>
        val remote = context.actorSelection("akka.tcp://BitCoinMining@" + ip + ":6000/user/master")
        remote ! giveWork
    }
  }


  // The Listener Actor
  class Listener extends Actor {
    def receive = {
      case aggregateHash(duration) ⇒
        println("Total Time : " + duration + " millisecs")
        context.system.shutdown()
    }
  }

  // The Master Actor
  class Master(payLoad : Int, loadPerWorker : Int, listener : ActorRef, noOfZeroes : Int) extends  Actor{
    // Determine the No Of Loads
    val noOfLoads = payLoad/loadPerWorker

    // Counter for Tracking the No of Loads
    var currentLoad = 0

    // Integrity Checks
    var noOfResults:Int = _

    // Start the Timer
    val start:Long = System.currentTimeMillis()

    def receive = {
      // Distribute the Work using the Router
      case StartMining =>
        // On Start Mining, Create (Workers = No of Cores) Server Workers and Start the Mining Process
        val noOfCores = Runtime.getRuntime.availableProcessors()

        for(i <- 1 until noOfCores) {
          context.actorOf(Props[Worker]) ! Work(currentLoad * loadPerWorker, loadPerWorker, noOfZeroes)
          currentLoad += 1
        }

      case Result(subList) =>
        // Increase the Results Counter
        noOfResults += 1

        // Print the Results
        subList.foreach(println(_))

        // Check the No Of Results and No of Loads
        if (noOfResults == noOfLoads) {
          // Send the Results Back to the Listener
          listener ! aggregateHash(System.currentTimeMillis - start)
        }

      // Handle GiveWork Message

      case `giveWork` =>
        // Give Work to the Sender
        if (currentLoad <= noOfLoads) {
          sender ! Work(currentLoad * loadPerWorker, loadPerWorker, noOfZeroes)
          currentLoad += 1
        }
    }
  }

  def main (args: Array[String]) {
    // Main Method
    // Based on the Argument, determine whether to run as a server or a worker
    val ipPattern = "^(?:[0-9]{1,3}\\.){3}[0-9]{1,3}$"
    if(args(0).matches(ipPattern)){
      // Connect to the Server
      startMiningFromRemote(args(0))
    }else{
      // Start the Server
      startMining(args(0).toInt)
    }
  }

  def startMining (noOfZeroes : Int){
    // Determine the Payload
    val payload = Math.pow(16, noOfZeroes + 1).toInt

    // Set the Load per Worker
    val loadPerWorker = 4000

    // Create an Akka system
    val system = ActorSystem("BitCoinMining")

    // create the result listener, which will print the result and shutdown the system
    val listener = system.actorOf(Props[Listener], name = "listener")

    // create the master
    val master = system.actorOf(Props(new Master(
      payload, loadPerWorker, listener, noOfZeroes)),
      name = "master")

    // start the calculation
    master ! StartMining
  }

  def startMiningFromRemote(ip : String) {
    // Create a Local Worker(s)
    val noOfCores = Runtime.getRuntime.availableProcessors()

    val localSystem = ActorSystem("localMiner")
    for (i <- 1 until noOfCores) {
      localSystem.actorOf(Props[Worker]) ! getWork(ip)
    }
  }
}
