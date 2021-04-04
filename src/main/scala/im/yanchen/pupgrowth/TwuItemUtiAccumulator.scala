package im.yanchen.pupgrowth

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
 * @author Yuting Yang
 * @create 2021-04-02-9:28
 */


class TwuItemUtiAccumulator extends AccumulatorV2[Sequence, mutable.Map[Int, (Long, Long)]] {
  var map: mutable.Map[Int, (Long, Long)] = mutable.Map()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[Sequence, mutable.Map[Int, (Long, Long)]] = new TwuItemUtiAccumulator

  override def reset(): Unit = map.clear()

  override def add(seq: Sequence): Unit = {
    for (entry <- seq.headTable) {
      var (item, pos) = entry
      var uti = 0
      val twu = seq.utility
      while (seq.hasNext(pos)) {
         val (itemUtil, temPos) = seq.getByIndexTuple(pos)
        pos = temPos
        if (uti < itemUtil)
          uti = itemUtil
      }

      var temp = (uti.toLong, twu.toLong)
      if (map.contains(item)) {
        val (preUtil, preTwu) = map.get(item).get
        temp = (temp._1 + preUtil, temp._2 + preTwu)
      }

      map(item) = temp
    }
  }

  override def merge(other: AccumulatorV2[Sequence, mutable.Map[Int, (Long, Long)]]): Unit = {
    val map1 = map
    val map2 = other.value

    map = map1.foldLeft(map2)(
      (innerMap, kv) =>{
        var temp = kv._2
        if(innerMap.contains(kv._1)){
          val temp1 = innerMap.get(kv._1).get
          temp = (temp._1 + temp1._1, temp._2 + temp1._2)
        }
        innerMap(kv._1) = temp
        innerMap
      }
    )
  }

  override def value: mutable.Map[Int, (Long, Long)] = map
}