package im.yanchen.pupgrowth

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer, Map}
import scala.util.control.Breaks._

/**
 * @author Yuting Yang
 * @create 2021-04-02-15:42
 */
class HUSP_ULL(val threshUtil: Long) {
  var HUSPs: mutable.ListBuffer[(String, Long)] = mutable.ListBuffer[(String, Long)]()

  def mine(seqs: Iterable[Sequence], glists: Map[Int, Int], gid: Int, itemUitAndTwu: mutable.Map[Int, (Long, Long)]): mutable.ListBuffer[(String, Long)] = {
    for ((item, _) <- glists) {
      if (glists(item) == gid) {
        val itemUtil = itemUitAndTwu(item)._1
        if (itemUtil >= threshUtil)
          HUSPs.append( (item+ " ", itemUtil))

        val itemSeqs = seqs.filter(
          _.headTable.contains(item)
        )
        println("fuck")
        if(itemSeqs.size > 0)
         itemSeqs.foreach(_.creatULL(Array(item), 'S'))
        PGrowth(Array(item), itemSeqs)
      }
    }
    HUSPs
  }


  def PGrowth(prefix: Array[Int], Seqs: Iterable[Sequence]): Unit = {
    iipFilter(Seqs, prefix)
    val (las_Imap, las_Smap) = lasFilter(Seqs, prefix)

    //I-Extension
    for((item,rsu) <- las_Imap if rsu >= threshUtil){
      Judge(prefix :+ item, Seqs, 'I')
    }

    //S-Extension
    for((item,rsu) <- las_Smap if rsu >= threshUtil){
      Judge(prefix :+ -1 :+ item, Seqs, 'S')
    }
  }

  def Judge(prefix: Array[Int], Seqs: Iterable[Sequence], char : Char) = {
    var (util, peu) = (0L,0L)

    for(seq <- Seqs){
      seq.creatULL(prefix, char)
      util += seq.prefixUtil
      peu += seq.peu
    }

    if(peu >= threshUtil){
      if(util >= threshUtil){
        HUSPs.append((prefix+(" "), util))
      }
      PGrowth(prefix, Seqs)

    }
  }


  def iipFilter(Seqs: Iterable[Sequence], prefix: Array[Int]) = {
    var iipMap = mutable.Map[Int, Long]()
    iipMap = Seqs.foldLeft(iipMap)((map, seq) => {
      val lastItem = prefix.last
      //      val lastItemList = ListBuffer[Int]()
      //      for((itemPos,_,_) <- seq.ull)
      //        lastItemList.append(itemPos._1)
      val startConcatPos = seq.ull(0)._1._1
      for ((item, pos) <- seq.headTable) {
        //both i-extension and s-extension available
        if (item > lastItem) {
          var tempPos = pos
          while (seq.hasNext(tempPos)) {
            if (tempPos._1 >= startConcatPos) {
              map(item) = map.getOrElse(item, 0L) + seq.peu
            }
            tempPos = seq.getUtil_NextPosByIndexTuple(tempPos)._2
          }
        }
        else {
          var tempPos = pos
          while (seq.hasNext(tempPos)) {
            if (tempPos._1 > startConcatPos)
              map(item) = map.getOrElse(item, 0L) + seq.peu
            tempPos = seq.getUtil_NextPosByIndexTuple(tempPos)._2
          }
        }
      }
      map
    })

    for ((item, peu) <- iipMap if peu < threshUtil) {
      for (seq <- Seqs) {
        seq.refineSeq(item)
      }
    }

  }

  def lasFilter(Seqs: Iterable[Sequence], prefix: Array[Int]): (mutable.Map[Int, Long], mutable.Map[Int, Long]) = {
    var las_Imap = mutable.Map[Int, Long]()
    var las_Smap = mutable.Map[Int, Long]()

    las_Smap = Seqs.foldLeft(las_Smap)((map, seq) => {
      val startConcatPos = seq.ull(0)._1._1
      for ((item, pos) <- seq.headTable) {
        var tempPos = pos
        breakable {
          while (seq.hasNext(tempPos)) {
            if (tempPos._1 > startConcatPos) {
              map(item) = map.getOrElse(item, 0L) + seq.peu
              break()
            }
            tempPos = seq.getUtil_NextPosByIndexTuple(tempPos)._2
          }
        }
      }
      map
    })

    las_Imap = Seqs.foldLeft(las_Imap)((map, seq) => {
      val lastItem = prefix.last
      for ((item, pos) <- seq.headTable if item > lastItem) {
        var tempPos = pos
        var ullIndex = 0
        val ull = seq.ull
        val up = seq.ull.size
        breakable {
          while (ullIndex < up && seq.hasNext(tempPos)) {
            val posx = tempPos._1
            val prefixPos = ull(ullIndex)._1._1
            if (posx == prefixPos) {
              map(item) = map.getOrElse(item, 0L) + seq.peu
              break()
            } else if (posx < prefixPos) {
              tempPos = seq.getUtil_NextPosByIndexTuple(tempPos)._2
            }
            else {
              ullIndex += 1
            }
          }
        }
      }
      map
    })
    (las_Imap, las_Smap)
  }

}
