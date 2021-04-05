package im.yanchen.pupgrowth

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.math.Ordered.orderingToOrdered
import util.control.Breaks._

class Sequence extends Cloneable {
  var utility: Int = 0
  var headTable: mutable.Map[Int, (Int, Int)] = mutable.Map()
  var UpInfo: mutable.ListBuffer[Array[((Int, Int), (Int, (Int, Int)))]] = mutable.ListBuffer()

  var prefix: Array[Int] = null
  var ull: mutable.ListBuffer[((Int, Int), Long, Long)] = mutable.ListBuffer[((Int, Int), Long, Long)]()
  var peu: Long = 0
  var prefixUtil: Long = 0

  def this(line: String) = {
    this()
    utility = line.substring(line.length - 2, line.length).toInt
    val itemsets = line.trim().split("-1").dropRight(1)
    var seqLen: Int = itemsets.length - 1
    var remainUti = 0

    val seq = itemsets.map(_.trim().split(" ").map(x => {
      val strings = x.split("\\[")
      (strings(0).toInt, strings(1).substring(0, strings(1).length - 1).toInt)
    }))

    var utiInfo: List[List[(Int, (Int, Int))]] = Nil
    while (seqLen >= 0) {
      var itemSetLen = seq(seqLen).length - 1
      var tempList: List[(Int, (Int, Int))] = Nil
      while (itemSetLen >= 0) {
        val (item, util) = seq(seqLen)(itemSetLen)
        val pos = headTable.getOrElse(item, (-1, -1))

        tempList = (remainUti, pos) :: tempList
        headTable.put(item, (seqLen, itemSetLen))
        remainUti += util
        itemSetLen -= 1
      }
      utiInfo = tempList :: utiInfo
      seqLen -= 1
    }

    for (i <- 0 until seq.length) {
      UpInfo.append(seq(i).zip(utiInfo(i)))
    }

  }

  override def clone(): Sequence = {
    var seq: Sequence = null
    try {
      //the father class will not clone the reference type fields
      seq = super.clone().asInstanceOf[Sequence]
      //manual clone the reference fields
      seq.headTable = seq.headTable.clone()
      seq.ull = seq.ull.clone()
      seq.UpInfo = seq.UpInfo.clone()
    } catch {
      case ex: Exception => println(ex.getMessage)
    }
    seq
  }

  /**
   * @param pos =(index1, index2)
   * @return (itemUtil, itemNextPos)
   */
  def getUtil_NextPosByIndexTuple(pos: (Int, Int)): (Int, (Int, Int)) = {
    val ((_, util), (_, position)) = UpInfo(pos._1)(pos._2)
    (util, position)
  }

  def getUti_RemainUtil_NextPosByIndexTuple(pos: (Int, Int)): (Int, Int, (Int, Int)) = {
    val ((_, util), (re, position)) = UpInfo(pos._1)(pos._2)
    (util, re, position)
  }

  def minusRemainUtilByIndexTuple(pos: (Int, Int), utilToMinus: Int) = {
    val ((item, util), (rem, position)) = UpInfo(pos._1)(pos._2)
    UpInfo(pos._1)(pos._2) = ((item, util), (rem - utilToMinus, position))
  }

  def hasNext(pos: (Int, Int)): Boolean = {
    if (pos == (-1, -1))
      false
    else
      true
  }

  def refineSeq(itemToDelete: Int) = {
    val pos: (Int, Int) = headTable.getOrElse(itemToDelete, (-1, -1))
    val prefixLastPos = headTable.get(prefix.last).get
    if (pos != (-1, -1)) {
      headTable -= itemToDelete
      var tempPos = pos
      while (tempPos < prefixLastPos && hasNext(tempPos)) {
        tempPos = getUtil_NextPosByIndexTuple(tempPos)._2
      }
      if (tempPos > prefixLastPos || (tempPos == prefixLastPos && prefix.last < itemToDelete)) {
        if (UpInfo(pos._1)(pos._2)._2._1 != 0) {

          val posMapUtilToMinus = getItemPosMapUtilToMinus(pos)
          updateSeqRemainUtil(posMapUtilToMinus)
          updateSeqUll(posMapUtilToMinus)
        }
      }
    }
  }

  def getItemPosMapUtilToMinus(pos: (Int, Int)): ListBuffer[((Int, Int), Int)] = {
    val posList = mutable.ListBuffer[(Int, Int)]()
    val utilList = mutable.ListBuffer[Int]()

    var varPos = pos
    var utilToMinus = 0
    while (hasNext(varPos)) {
      val ((_, util), re) = UpInfo(varPos._1)(varPos._2)
      utilToMinus += util

      posList += varPos
      utilList += utilToMinus
      varPos = re._2
    }

    var i = utilList.size - 2
    while (i >= 0) {
      utilList(i) += utilList(i + 1)
      i -= 1
    }
    val posMapUtilToMinus = posList zip utilList
    posMapUtilToMinus
  }

  def updateSeqRemainUtil(posMapUtilToMinus: ListBuffer[((Int, Int), Int)]): Unit = {
    var index = 0
    val upIndex = posMapUtilToMinus.size - 1
    for ((_, pos) <- headTable) {
      var varPos = pos
      while (hasNext(varPos) && index <= upIndex) {
        val (anchor, utilToMinus) = posMapUtilToMinus(index)
        if (varPos < anchor) {
          minusRemainUtilByIndexTuple(varPos, utilToMinus)
          varPos = getUtil_NextPosByIndexTuple(varPos)._2
          index +=1
        }
        else{
          index += 1
        }
      }
      index = 0
    }

  }

  def updateSeqUll(posMapUtilToMinus: ListBuffer[((Int, Int), Int)]): Unit = {
    var mapIndex = 0
    val mapUpIndex = posMapUtilToMinus.size - 1

    var ullIndex = 0
    val ullUpIndex = ull.size - 1

    while (mapIndex <= mapUpIndex && ullIndex <= ullUpIndex){
      val (pos, uti, tempPeu) = ull(ullIndex)
      val (anchor, utilToMinus) = posMapUtilToMinus(mapIndex)
      if (pos < anchor) {
        if (peu == tempPeu)
          peu -= utilToMinus
        ull(ullIndex) = (pos, uti, tempPeu - utilToMinus)
        ullIndex += 1
        mapIndex += 1
      }
      else {
        mapIndex += 1
      }
    }
  }

  def creatULL(newPrefix: Array[Int], exType: Char): Unit = {
    var tempUtil = 0L
    var tempPeu = 0L
    val exItem = newPrefix.last

    if (prefix == null) {
      var pos = headTable(exItem)
      if (headTable.contains(exItem)) {
        while (hasNext(pos)) {
          val (uti, remain, temPos) = getUti_RemainUtil_NextPosByIndexTuple(pos)
          val peu = if (remain > 0) uti + remain else 0L

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
      if (exType == 'I') {
        val newList = mutable.ListBuffer[((Int, Int), Long, Long)]()
        var oldUllIndex = 0
        val oldUllSize = ull.size
        var tempPos = pos

        while (oldUllIndex < oldUllSize && hasNext(tempPos)) {
          val ((oldPos, _), olduti, _) = ull(oldUllIndex)
          if (tempPos._1 == oldPos) {
            val ((_, util), (re, _)) = UpInfo(tempPos._1)(tempPos._2)
            val eleUtil = olduti + util
            val elePeu = if (re == 0L) 0 else re + eleUtil
            newList.append((tempPos, eleUtil, elePeu))
            if (tempUtil < eleUtil) {
              tempUtil = eleUtil
              tempPeu = elePeu
            }

            tempPos = getUtil_NextPosByIndexTuple(tempPos)._2
            oldUllIndex += 1
          }
          else if (tempPos._1 < oldPos) {
            tempPos = getUtil_NextPosByIndexTuple(tempPos)._2
          }
          else {
            oldUllIndex += 1
          }
        }
        ull = newList
      } //S-Extension
      else if (exType == 'S') {
        val newList = mutable.ListBuffer[((Int, Int), Long, Long)]()
        var oldUllIndex = 0
        val oldUllSize = ull.size
        var tempPos = pos
        var eleUtil = 0L

        while (oldUllIndex < oldUllSize && hasNext(tempPos)) {
          val ((oldPos, _), olduti, _) = ull(oldUllIndex)

          if (tempPos._1 > oldPos) {
            eleUtil = if (eleUtil < olduti) olduti else eleUtil
            if (oldUllIndex == oldUllSize - 1) {
              val ((_, util), (re, _)) = UpInfo(tempPos._1)(tempPos._2)
              eleUtil += util
              val elePeu = if (re == 0L) 0L else re + eleUtil
              newList.append((tempPos, eleUtil, elePeu))

              if (tempUtil < eleUtil) {
                tempUtil = eleUtil
                tempPeu = elePeu
              }
            }

            oldUllIndex += 1
          }
          else if (tempPos._1 < oldPos) {
            val ((_, util), (re, _)) = UpInfo(tempPos._1)(tempPos._2)
            eleUtil += util
            val elePeu = if (re == 0L) 0L else re + eleUtil
            newList.append((tempPos, eleUtil, elePeu))

            if (tempUtil < eleUtil) {
              tempUtil = eleUtil
              tempPeu = elePeu
            }

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