package im.yanchen.pupgrowth

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import util.control.Breaks._

class Sequence {
  var utility: Int = 0
  val headTable: mutable.Map[Int, (Int, Int)] = mutable.Map()
  val UpInfo : mutable.ListBuffer[Array[((Int, Int), (Int, (Int, Int)))]] = mutable.ListBuffer()

  var prefix : Array[Int] = null
  var ull : mutable.ListBuffer[((Int,Int), Long, Long)] = mutable.ListBuffer[((Int,Int), Long, Long)]()
  var peu: Long = 0
  var prefixUtil: Long = 0

  def this(line: String) = {
    this()
    utility = line.substring(line.length - 2, line.length).toInt
    val itemsets= line.trim().split("-1").dropRight(1)
    var seqLen : Int = itemsets.length - 1
    var remainUti = 0

    val seq= itemsets.map(_.trim().split(" ").map( x => {
      val strings = x.split("\\[")
      (strings(0).toInt, strings(1).substring(0, strings(1).length - 1).toInt)
    }))

    var utiInfo :List[List[(Int, (Int,Int))]]= Nil
    while (seqLen >= 0){
      var itemSetLen = seq(seqLen).length - 1
      var tempList : List[(Int, (Int,Int))] = Nil
      while (itemSetLen >= 0){
        val (item, util) = seq(seqLen)(itemSetLen)
        val pos = headTable.getOrElse(item, (-1, -1))

        tempList = (remainUti, pos) :: tempList
        headTable.put(item, (seqLen, itemSetLen))
        remainUti += util
        itemSetLen -= 1
      }
      utiInfo = tempList :: utiInfo
      seqLen -=1
    }

    for(i <- 0 until  seq.length) {
      UpInfo.append(seq(i).zip(utiInfo(i)))
    }

  }

  /**
   * @param pos=(index1, index2)
   * @return (itemUtil, itemNextPos)
   */
  def getUtil_NextPosByIndexTuple(pos : (Int, Int)) : (Int, (Int,Int)) ={
    val ((_,util),(_ , position)) = UpInfo(pos._1)(pos._2)
    (util, position)
  }

  def getUti_RemainUtil_NextPosByIndexTuple(pos : (Int, Int)) : (Int, Int, (Int,Int)) ={
    val ((_,util),(re, position)) = UpInfo(pos._1)(pos._2)
    (util, re, position)
  }

  def minusRemainUtilByIndexTuple(pos : (Int, Int), utilToMinus : Int) ={
    val ((item,util),(rem , position)) = UpInfo(pos._1)(pos._2)
    UpInfo(pos._1)(pos._2) = ((item,util),(rem - utilToMinus , position))
  }

  def hasNext(pos : (Int, Int)) : Boolean ={
    if(pos == (-1,-1))
      false
    else
      true
  }

  def refineSeq(itemToDelete : Int)={
    val pos: (Int, Int) = headTable.getOrElse(itemToDelete, (-1, -1))
    if(pos != (-1,-1)){
      headTable - itemToDelete
      val posMapUtilToMinus = getItemPosMapUtilToMinus(pos)
      updateSeqRemainUtil(posMapUtilToMinus)
      updateSeqUll(posMapUtilToMinus)
    }
  }

  def getItemPosMapUtilToMinus(pos : (Int, Int)): ListBuffer[((Int, Int), Int)] ={
    val posList = mutable.ListBuffer[(Int,Int)]()
    val utilList = mutable.ListBuffer[Int]()

    var varPos = pos
    var utilToMinus = 0
    while (hasNext(varPos)){
      val ((_,util),re) = UpInfo(varPos._1)(varPos._2)
      utilToMinus += util

      posList += varPos
      utilList += utilToMinus
      varPos = re._2
    }
    val posMapUtilToMinus = posList zip (utilList.reverse)
    posMapUtilToMinus
  }

  def updateSeqRemainUtil( posMapUtilToMinus : ListBuffer[((Int, Int), Int)]): Unit ={
    for((_,pos) <- headTable){
      var varPos = pos
      while (hasNext(varPos)){
        breakable{
          for((anchor, utilToMinus) <- posMapUtilToMinus){
            if(varPos._1 < anchor._1 || (varPos._1 == anchor._1 && varPos._2 < anchor._2)){
              minusRemainUtilByIndexTuple(varPos, utilToMinus)
              break()
            }
          }
        }
      }
      varPos = getUtil_NextPosByIndexTuple(varPos)._2
    }
  }

  def updateSeqUll( posMapUtilToMinus : ListBuffer[((Int, Int), Int)]): Unit ={
    for(i <- 0 until ull.size){
      val (pos,uti,tempPeu) = ull(i)
      breakable{
        for((anchor, utilToMinus) <- posMapUtilToMinus){
          if(pos._1 < anchor._1 || (pos._1 == anchor._1 && pos._2 < anchor._2)){
            if(peu == tempPeu)
              peu -= utilToMinus
            ull(i) = (pos, uti, tempPeu - utilToMinus)
            break()
          }
        }
      }
    }
  }

  def creatULL(newPrefix: Array[Int], exType: Char): Unit ={
    var tempUtil = 0L
    var tempPeu = 0L
    val exItem = newPrefix.last

    if(prefix == null) {
      var pos = headTable(exItem)
      if (headTable.contains(exItem)) {
        while (hasNext(pos)) {
          val (uti, remain, temPos) = getUti_RemainUtil_NextPosByIndexTuple(pos)
          val peu = if (remain > 0) uti + remain else 0

          if (uti > tempUtil) tempUtil = uti
          if (peu > tempPeu) tempPeu = peu

          ull.append((pos, uti, peu))
          pos = temPos
        }
      }
    }
    else {
      val pos = headTable(exItem)

      //I-Extension
      if(exType == 'I'){
        val newList = mutable.ListBuffer[((Int,Int),Long,Long)]()
        var oldUllIndex = 0
        val oldUllSize = ull.size
        var tempPos = pos

        while (oldUllIndex < oldUllSize && hasNext(tempPos)){
          val ((oldPos,_), olduti, _) = ull(oldUllIndex)
          if(tempPos._1 == oldPos){
            val ((_,util),(re,_)) = UpInfo(tempPos._1)(tempPos._2)
            val eleUtil = olduti + util

            newList.append((tempPos, eleUtil, re))
            if(tempUtil < eleUtil) tempUtil = eleUtil
            if(tempPeu < re) tempPeu = re

            tempPos = getUtil_NextPosByIndexTuple(tempPos)._2
            oldUllIndex += 1
          }
          else if(tempPos._1 < oldPos){
            tempPos = getUtil_NextPosByIndexTuple(tempPos)._2
          }
          else {
            oldUllIndex += 1
          }
        }
        ull = newList
      }//S-Extension
      else if(exType == 'S'){
        val newList = mutable.ListBuffer[((Int,Int),Long,Long)]()
        var oldUllIndex = 0
        val oldUllSize = ull.size
        var tempPos = pos
        var eleUtil = 0L

        while (oldUllIndex < oldUllSize && hasNext(tempPos)){
          val ((oldPos,_), olduti, _) = ull(oldUllIndex)

          if(tempPos._1 > oldPos){
            eleUtil = if(eleUtil < olduti) olduti else eleUtil
            if(oldUllIndex == oldUllSize - 1){
              val ((_,util),(re,_)) = UpInfo(tempPos._1)(tempPos._2)
              eleUtil += util
              val elePeu = re + eleUtil
              if(tempUtil < eleUtil) tempUtil = eleUtil
              if(tempPeu < elePeu) tempPeu = elePeu
              newList.append((tempPos, eleUtil, re))
            }

            oldUllIndex += 1
          }
          else if(tempPos._1 < oldPos){
            val ((_,util),(re,_)) = UpInfo(tempPos._1)(tempPos._2)
            eleUtil += util
            val elePeu = re + eleUtil
            if(tempUtil < eleUtil) tempUtil = eleUtil
            if(tempPeu < elePeu) tempPeu = elePeu
            newList.append((tempPos, eleUtil, re))
            eleUtil = 0
            tempPos = getUtil_NextPosByIndexTuple(tempPos)._2
          }
          else {
            tempPos = getUtil_NextPosByIndexTuple(tempPos)._2
          }
        }
        ull = newList
      }
      else {
        println("illegal extension type")
      }
    }

    prefixUtil = tempUtil
    peu = tempPeu
    prefix = newPrefix
  }


}