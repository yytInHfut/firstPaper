package im.yanchen.pupgrowth.USpan

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._
/**
 * @author Yuting Yang
 * @create 2021-04-22-9:51
 */
object AppUspan {
  private var numOfHusps = 0

  def main(args: Array[String]): Unit = {
//    MemoryLogger.getInstance().reset()
    val startTime: Long = System.currentTimeMillis()
//    val parNum = args(2).toInt
//    val method = args(3).toInt // 0: print stats; 1: PUPGrowth; 2: PUPGrowth with sampling
//    val sperc = args(4).toDouble // sample size
//    val outputf = args(5)

    val fileName = args(0)
    val theta = args(1).toDouble
    val parNum = args(2).toInt

    val conf = new SparkConf().setAppName("PUspan").setMaster("local[1]")
//val conf = new SparkConf().setAppName("PUspan")

    val sc = new SparkContext(conf)
    val totalUtility: LongAccumulator = sc.longAccumulator("totalUtility")
    //    var lines = sc.textFile(args(0)).repartition(parNum)
    val lines = sc.textFile("datas/"+fileName+".txt").repartition(parNum)

    val originUspan: RDD[AlgoUSpan] = lines.mapPartitions(data => {
      val uSpan = new AlgoUSpan()
      uSpan.getqMatrices(data.toBuffer.asJava, theta)
      Iterator(uSpan)
    })

    originUspan.foreach(x => {
      totalUtility.add(x.minUtility)
    })

    System.out.println("totalUtil = " + totalUtility.value)
//    originUspan.persist()

    val localTrees: RDD[TreeNode] = originUspan.mapPartitionsWithIndex((nodeId, originUspan ) => {
      val uSpan = originUspan.next()
      uSpan.runAlgorithm( nodeId)
      //      uSpan.result.asScala.toIterator
      val list = Iterator(uSpan.theTree)
      list
    })

    localTrees.persist();
//    while (result.getNumPartitions != 1){
//      result = result.coalesce(result.getNumPartitions/2)
//      result.mapPartitions( (gTree) =>{
//
//        gTree
//      })
//    }

    val globalTree = new GlobalTree((totalUtility.value).toLong)
    globalTree.treeNode = localTrees.map(new GTreeNode(_)).reduce((x, y) => {
      val globalTree = new GlobalTree((totalUtility.value).toLong)
      globalTree.mergeTrees( globalTree, Iterator(x,y).asJava)
      globalTree.treeNode
    })

    globalTree.printTree()
    numOfHusps += GlobalTree.numOfHusps
    globalTree.getCandidates()
//    globalTree.candTree.printTree()
    val candTree: GlobalTree#candidateTreeNode = globalTree.candTree

    val candTreeWithValues: RDD[GlobalTree#candidateTreeNode] = localTrees.map(loTree => {
      loTree.secondaryCompute(candTree)
      candTree
    })

    localTrees.unpersist()

    val mergedCandidateTree = candTreeWithValues.reduce((x,y)=> {
      merge(x,y)
      x
    })
    System.out.println("*********************")
    System.out.println("")
    System.out.println("Num of Husps mined in firstRound: " + numOfHusps)

    printHUSP(mergedCandidateTree, totalUtility.value, new Array[Int](100), 0)
    System.out.println("Total num of Husps: " + numOfHusps)


    //the "print function" of java and scala can't mixed
//    println(globalTree.candidate.size())
//    for(cand <- globalTree.candidate.asScala){
//      print("df")
//      println(cand)
//    }
//    globalTree.printTree()
//    localTrees.collect().foreach(_.printTree())
val endTime: Long = System.currentTimeMillis()
    System.out.println("endTime - startTime = " + (endTime - startTime))
    sc.stop()
  }

  def printHUSP(cand : GlobalTree#candidateTreeNode, util : Long, prefix: Array[Int], length : Int): Unit = {
    if(cand.children.size() > 0){
      for (n <- cand.children.asScala) {
        prefix(length) = n.item
        if (n.isPatternCand && n.utility >= util){
          numOfHusps += 1
          for(i <-0 to length){
            System.out.print(i)
          }
          System.out.println(n.utility)
        }
        printHUSP(n, util, prefix, length + 1)
      }
    }
  }

  def merge(node: GlobalTree#candidateTreeNode, other: GlobalTree#candidateTreeNode): Unit = {
    var curNode: GlobalTree#candidateTreeNode = null
    var curOhterNode: GlobalTree#candidateTreeNode = null
    for (i <- 0 to node.childrenArray.size - 1) {
      curNode = node.children.get(i)
      curOhterNode = other.children.get(i)
      if (curNode.item != -1 && curNode.isPatternCand) curNode.utility += curOhterNode.utility
      merge(curNode, curOhterNode)
    }
  }
}
