package im.yanchen.pupgrowth

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer, Map}

/**
 * @author Yuting Yang
 * @create 2021-04-02-15:42
 */
class HUSP_ULL(val threshUtil: Long) {
  var HUSPs: ArrayBuffer[(Int, Int)] = ArrayBuffer()

  def mine(seqs: Iterable[Sequence], glists: Map[Int, Int], gid: Int, itemUitAndTwu: mutable.Map[Int, (Long, Long)]): Iterator[(Int, Int)] = {
    for ((item, _) <- glists) {
      if (glists(item) == gid) {
        val itemUtil = itemUitAndTwu(item)._1
        if (itemUtil >= threshUtil)
          HUSPs += (item, itemUtil)

        val itemSeqs = seqs.filter(
          _.headTable.contains(item)
        )
        PGrowth(new Array[Int](item), itemSeqs)
      }
    }

    HUSPs.iterator
  }

  def PGrowth(prefix: Array[Int], Seqs: Iterable[Sequence]): Unit = {
    iipFilter(Seqs, prefix)

  }

  def iipFilter(Seqs: Iterable[Sequence], prefix: Array[Int]) = {
    var iipMap = mutable.Map[Int, Long]()
    val las_Imap = mutable.Map[Int,Long]()
    val las_Smap = mutable.Map[Int,Long]()

    iipMap = Seqs.foldLeft(iipMap)((map, seq) => {
      val lastItem = prefix.last
      val lastItemList = ListBuffer[Int]()
      for((itemPos,_,_) <- seq.ull)
        lastItemList.append(itemPos._1)
      var (iipFlag, iFlag, sFlag) = (false, false,false)
      for ((item, pos) <- seq.headTable){
        //both i-extension and s-extension available
        if(item > lastItem){
          var tempPos = pos
          while (seq.hasNext(tempPos)){

          }
        }


        map(item) = iipMap.getOrElse(item, 0) + seq.peu
      }
      map
    })

    for((item, peu) <- iipMap if peu < threshUtil){
      for(seq <- Seqs){
        seq.refineSeqRemainUtil(item)
      }
    }

  }

}
