package com.wifi.recommend

import com.wifi.recommend.Similarity.Similarity
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object Similarity extends Enumeration {
  type Similarity = Value
  val JACCARD, COSINE = Value
}

/**
 * Author: liujikun@xiaomi.com
 * Date: 2017-03-17.
 *
 * @param input RDD[(user, item)]
 * @param minUserHistory 忽略items太少的用户，默认2
 * @param maxUserHistory 截断用户太长的items，默认1000
 * @param itemSimNum item to item top num，默认100
 * @param userRecNum user to item top num，默认100
 * @param minSim 最小相似度，默认0
 * @param minCooccur 最小共现次数，默认2
 */


class CollaborativeFiltering(
                              input: RDD[(String, String, Double)],
                              itemSimNum: Int,
                              userRecNum: Int,
                              minSim: Double,
                              minUserHistory: Int,
                              maxUserHistory: Int,
                              minItemHistory: Int,
                              minCooccur: Int,
                              partitions: Int,
                              similarity: Similarity
                              )
  extends Serializable {
  def this(input: RDD[(String, String)]) =
    this(input.map(x => (x._1, x._2, 1.0)), 100, 100, 0, 2, 1000, 10, 2, 5000, Similarity.JACCARD)

  def this(input: RDD[(String, String, Double)], similarity: Similarity) =
    this(input, 100, 100, 0, 2, 1000, 10, 2, 5000, similarity)

  private var simList_ : RDD[(String, Array[(String, Double)])] = _

  private val userHistory = input.coalesce(partitions).map {
    case (user, item, rating) =>
      if (similarity == Similarity.JACCARD) {
        (user, (item, 1.0))
      } else {
        (user, (item, rating))
      }
  }.groupByKey()
    .map { case (user, items) => (user, items.take(maxUserHistory).toMap) }
    .filter(_._2.size >= minUserHistory)
    .cache()

  /**
   *
   * @return RDD[(item, (item, sim))]
   */
  private def simPair(): RDD[(String, (String, Double))] = {

    val itemsCount = userHistory.flatMap {
      case (user, items) => items.map(item => (item._1, item._2 * item._2))
    }.reduceByKey(_ + _)
      .filter(_._2 >= minItemHistory)
      .collectAsMap()

    val matrix = userHistory.flatMap {
      case (user, items) =>
        val it = items.toSeq.sortWith(_._1 < _._1)
        for (i <- it.indices; j <- it.indices.drop(i + 1)) yield {
          ((it(i)._1, it(j)._1), it(i)._2 * it(j)._2)
        }
    }.reduceByKey(_ + _).filter(_._2 >= minCooccur)

    matrix.map {
      case ((i, j), dot) =>
        val sim = {
          if (itemsCount.contains(i) && itemsCount.contains(j)) {
            if (similarity == Similarity.JACCARD) {
              dot / (itemsCount(i) + itemsCount(j) - dot)
            } else {
              dot / Math.sqrt(itemsCount(i) * itemsCount(j))
            }
          }
          else 0.0
        }
        (i, (j, sim))
    }.filter(_._2._2 > minSim).flatMap {
      case (i, (j, sim)) =>
        Seq((i, (j, sim)), (j, (i, sim)))
    }
  }

  /**
   * @return RDD[(item, Array(item, sim))]
   */
  def simList(): RDD[(String, Array[(String, Double)])] = {
    if (simList_ != null)
      return simList_

    simList_ = simPair().groupByKey().map {
      case (i, iter) =>
        val simItems = iter.toArray.sortWith(_._2 > _._2).take(itemSimNum)
        (i, simItems)
    }
    simList_.cache()
  }

  /**
   * @return RDD[(user, Array(item, sim))]
   */
  def userRecommend(): RDD[(String, Array[(String, Double)])] = {
    userHistory.flatMap {
      case (user, items) => items.map { case (item, rating) => (item, (user, rating)) }
    }.join(simList()).map {
      case (item, ((user, rating), simItemList)) =>
        val fullItemList = simItemList.map { case (simItem, sim) => (rating, simItem, sim) }
        (user, fullItemList)
    }.reduceByKey(_ ++ _).join(userHistory).map {
      case (user, (fullItemList, userHistoryMap)) =>
        val itemRecsMap = mutable.HashMap[String, Double]()
        fullItemList.foreach {
          case (rating, simItem, sim) =>
            if (!userHistoryMap.contains(simItem)) {
              if (!itemRecsMap.contains(simItem)) {
                itemRecsMap(simItem) = sim * rating
              } else {
                itemRecsMap(simItem) += sim * rating
              }
            }
        }

        val itemRecsList = itemRecsMap.toArray.sortWith(_._2 > _._2).take(userRecNum)
        (user, itemRecsList)
    }
  }

}