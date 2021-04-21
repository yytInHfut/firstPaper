package im.yanchen.pupgrowth

import org.apache.spark.util.{BoundedPriorityQueue, Utils}

import scala.collection.mutable._

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd._

import scala.collection.mutable

/**
 * @author Yan Chen
 */
object App {
  def grouping(items: List[Int], pnum: Int): Map[Int, Int] = {

    var mp = Map[Int, Int]()
    if (pnum == 1) {
      for (i <- items)
        mp.put(i, 0)
    } else {
      var i = 0
      var inc = 1
      var flag = false
      for (x <- items) {
        mp.put(x, i)
        i += inc
        if (i == 0 || i == pnum - 1) {
          if (flag == false) {
            inc = 0
            flag = true
          } else {
            if (i == 0)
              inc = 1
            else
              inc = -1
            flag = false
          }
        }
      }
    }

    mp
  }

  def grouping2(items: List[Int], pnum: Int): Map[Int, Int] = {

    var mp = Map[Int, Int]()
    val r = scala.util.Random

    for (x <- items) {
      mp.put(x, r.nextInt(pnum))
    }
    mp
  }

  def main(args: Array[String]) {

    val startTimestamp = System.currentTimeMillis()

    val theta = args(1).toDouble
    val parNum = args(2).toInt
    val method = args(3).toInt // 0: print stats; 1: PUPGrowth; 2: PUPGrowth with sampling
    val sperc = args(4).toDouble // sample size
    val outputf = args(5)

    val conf = new SparkConf().setMaster("local[*]").setAppName("PUPGrowth")
      .set("spark.executor.memory", "28G")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "im.yanchen.pupgrowth.ARegistrator")
      .set("spark.kryoserializer.buffer.mb", "24")
      .set("spark.storage.blockManagerHeartBeatMs", "30000000")
    val sc = new SparkContext(conf)

    //    var lines = sc.textFile(args(0)).repartition(parNum)
    val lines = sc.textFile(args(0))
    lines.mapPartitionsWithIndex((nodeId, data) => )
    val seqRDD: RDD[Sequence] = lines.map(new Sequence(_))
    seqRDD.persist()

    // compute item TWU, Utility

    //utilize custom Accumulator to calculate item utility and TWU
    //utilize LongAccumulator to calculate the total utility of the database
    val totalUtil = sc.longAccumulator("totalUtil")
    val itemUtilAndTwu = new TwuItemUtiAccumulator
    sc.register(itemUtilAndTwu, "statistic")
    seqRDD.foreach(
      seq => {
        // 使用累加器
        itemUtilAndTwu.add(seq)
        totalUtil.add(seq.utility)
      })
    //revise the sequence by deleting the low TWU item, and update the seqRDD simultaneously
    val threshUtil = (totalUtil.value * theta).toLong
    val lowTwuItemlist = ListBuffer[Int]()
    val refinedItemUtilAndTwu: mutable.Map[Int, (Long, Long)] = mutable.Map()
    for ((item, (uti, twu)) <- itemUtilAndTwu.value) {
      if (twu >= threshUtil)
        refinedItemUtilAndTwu(item) = (uti, twu)
      else
        lowTwuItemlist.append(item)
    }

    val refinedSeq = seqRDD.map(
      seq => {
        for (i <- lowTwuItemlist) {
          seq.refineSeq(i)
        }
        seq
      }
    )

    //create the global Broadcast variable of item TWU, utility and utility threshold
    val threshBroad = sc.broadcast(threshUtil)

    //refine the itemList by the TWU
    val itemTwuAndUtilBroad = sc.broadcast(refinedItemUtilAndTwu)

    val items = refinedItemUtilAndTwu.toList.sortBy(x => x._2._2).map(x => x._1)

    val glists = grouping(items, parNum)
    val glistsBroad = sc.broadcast(glists)

    //Redistribute data to nodes
    var kset = refinedSeq.flatMap { x => {
      val tlist = ArrayBuffer[(Int, Sequence)]()
      val added = Map[Int, Boolean]()
      for ((i, _) <- x.headTable) {
        val gid = glistsBroad.value(i)
        if (added.get(gid) == None) {
          tlist.append((gid, x))
          added(gid) = true
        }
      }
      tlist.iterator
    }
    }
    kset.persist()
    seqRDD.unpersist(false)
    if (parNum != 1)
      kset = kset.partitionBy(new BinPartitioner(parNum))
    val gset = kset.groupByKey()

    //execute the pattern miner program
    val results = gset.flatMap(x => {
      val hm = new HUSP_ULL(threshBroad.value)
      hm.mine(x._2, glistsBroad.value, x._1, itemTwuAndUtilBroad.value)
    })

    val fresults = results.collect()

    val endTimestamp = System.currentTimeMillis()

    println("glists: ")
    for ((key, value) <- glists) {
      println("\t" + key + ": " + value)
    }
    println("Thres: " + threshUtil)
    println("Total HUIs: " + fresults.size)
    println("Running time: " + (endTimestamp - startTimestamp))

    //          var writer = new BufferedWriter(new FileWriter(outputf, true))
    //
    //          writer.write("Running time: " + (endTimestamp - startTimestamp) + "\n")
    //          writer.write("Total HUIs: " + fresults.size + "\n")
    //          writer.write("utotal: " + utotal + "\n")
    //          writer.write("thresUtil: " + thresUtil.toInt + "\n")
    //          writer.write("parNum: " + parNum + "\n")
    //
    //          for ((key, value) <- fresults) {
    //            writer.write("" + key + "#UTIL: " + value + "\n")
    //          }
    //
    //          writer.write("\n******************************" + "\n")
    //
    //          writer.close()

    sc.stop()


    //    if (method == 2) {
    //      lines = lines.sample(false, sperc, 0)
    //    }
    //    val transacs = lines.map(s => new Transaction(s))
    //    transacs.persist()
    //
    //    // use count here for executing revisedTransacs
    //    // compute statistics
    //    val (utotal, tnum) = transacs.mapPartitions(x => {
    //      var maxutil = 0L
    //      var totalutil = 0L
    //      var count = 0L
    //      while (x.hasNext) {
    //        val ele = x.next()
    //        totalutil += ele.utility
    //        count += 1
    //      }
    //      Array((totalutil, count)).iterator
    //    }).reduce((x, y) => (x._1 + y._1, x._2 + y._2))
    //
    //    // compute item TWU, and filter unpromising ones
    //    val thresUtil = theta * utotal
    //    val thresUtilBroad = sc.broadcast(thresUtil)
    //    val itemTwu = transacs.flatMap(x => x.itemset.map(y => (y._1, x.utility)))
    //      .reduceByKey(_ + _)
    //      .filter(x => x._2 >= thresUtilBroad.value)
    //      .collect()
    //      .toMap
    //    val itemTwuBroad = sc.broadcast(itemTwu)
    //    val revisedTransacs = transacs.map(t => {
    //      t.itemset = t.itemset.filter(x => itemTwuBroad.value.get(x._1) != None)
    //      Sorting.quickSort(t.itemset)(Ordering[(Int, Int)].on(x => (itemTwuBroad.value.get(x._1).get, x._1)))
    //      t
    //    }).filter(x => x.itemset.size >= 1)
    //    revisedTransacs.persist()
    //
    //    transacs.unpersist(false)
    //
    //    val items = itemTwu.toList.sortBy(x => x._2).map(x => x._1)
    //
    //    val glists = grouping(items, parNum)
    //    val glistsBroad = sc.broadcast(glists)
    //
    //    if (method == 0) {
    //      var writer = new BufferedWriter(new FileWriter(outputf))
    //      writer.write("The number of transactions is " + tnum + "\n")
    //      writer.write("items list: " + items.mkString(" ") + "\n")
    //      writer.write("glists are\n")
    //      for ((key, value) <- glists) {
    //        writer.write("" + key + ":" + value + "\n")
    //      }
    //      writer.close()
    //
    //    } else if (method == 1 || method == 2) {
    //      var kset = revisedTransacs.flatMap { x => {
    //        var tlist = ArrayBuffer[(Int, Array[(Int, Int)])]()
    //        var added = Map[Int, Boolean]()
    //        for (i <- 0 to x.length-1) {
    //          var firstitem = x.itemset(i)._1
    //          var gid = glistsBroad.value(firstitem)
    //          if (added.get(gid) == None) {
    //            tlist.append((gid, x.itemset.slice(i, x.length)))
    //            added(gid) = true
    //          }
    //        }
    //        tlist.iterator
    //      } }
    //      kset.persist()
    //      revisedTransacs.unpersist(false)
    //
    //      if(parNum != 1)
    //        kset = kset.partitionBy(new BinPartitioner(parNum))
    //      val gset = kset.groupByKey()
    //      val results = gset.flatMap(x => {
    //        var hm = new HUIMiner(itemTwuBroad.value)
    //        for (transac <- x._2) {
    //          hm.addTransac(transac)
    //        }
    //
    //        hm.mine(thresUtilBroad.value.toInt, glistsBroad.value, x._1)
    //      })
    //
    //      val fresults = results.collect().toMap
    //
    //      var endTimestamp = System.currentTimeMillis()
    //
    ////      println("glists: ")
    ////      for ((key, value) <- glists) {
    ////        println("\t" + key + ": " + value)
    ////      }
    //      println("Thres: " + thresUtil.toInt)
    //      println("Total HUIs: " + fresults.size)
    //      println("Running time: " + (endTimestamp - startTimestamp))
    //
    //      var writer = new BufferedWriter(new FileWriter(outputf, true))
    //
    //      writer.write("Running time: " + (endTimestamp - startTimestamp) + "\n")
    //      writer.write("Total HUIs: " + fresults.size + "\n")
    //      writer.write("utotal: " + utotal + "\n")
    //      writer.write("thresUtil: " + thresUtil.toInt + "\n")
    //      writer.write("parNum: " + parNum + "\n")
    //
    //      for ((key, value) <- fresults) {
    //        writer.write("" + key + "#UTIL: " + value + "\n")
    //      }
    //
    //      writer.write("\n******************************" + "\n")
    //
    //      writer.close()
    //    } else {
    //      println("Warning: method not implemented!")
    //    }
    //
    //  }
    //
    //  def updateExactUtility(transac: Array[(Int, Int)], itemset: Array[Int], mapItemToTWU: scala.collection.immutable.Map[Int, Int], results: Map[Array[Int], Int]) {
    //    var utility = 0
    //
    //    for (i <- 0 to itemset.length-1) {
    //      breakable {
    //        var itemI = itemset(i)
    //        for (j <- 0 to transac.length-1) {
    //          var itemJ = transac(j)
    //
    //          if (itemJ._1 == itemI) {
    //            utility += itemJ._2
    //            break
    //          } else if (mapItemToTWU(itemJ._1) < mapItemToTWU(itemI)) {
    //            return
    //          }
    //
    //        }
    //        return
    //      }
    //    }
    //
    //    if (!results.contains(itemset)) {
    //      results(itemset) = 0
    //    }
    //    results(itemset) += utility
    //
  }

}
