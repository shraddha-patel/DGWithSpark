package org.finra.datagenerator

import java.util
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.log4j.Logger
import org.finra.datagenerator.engine.Frontier

/**
 * Created by k25284 on 9/23/2015.
 */
class searchWorker(var frontier: Frontier, var randomNumberQueue: util.LinkedList[util.Map[String, String]], var searchExitFlag: AtomicBoolean) extends Runnable{
  val log: Logger = Logger.getLogger(classOf[searchWorker])
  //var frontier: Frontier = null
  //var randomNumberQueue: util.LinkedList[util.Map[String, String]] = null
  //var flag: AtomicBoolean = null

  def SearchWorker(frontier: Frontier, randomNumberQueue: util.LinkedList[util.Map[String,String]], flag: AtomicBoolean) {
    this.randomNumberQueue = randomNumberQueue
    this.frontier = frontier
    this.searchExitFlag = flag
  }

  override def run(): Unit = {
    log.info(Thread.currentThread.getName + " is starting DFS")
    frontier.searchForScenarios(randomNumberQueue, searchExitFlag)
    log.info(Thread.currentThread.getName + " is done with DFS")
  }
}
