package app.recommender.baseline

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK

class BaselinePredictor() extends Serializable {

  private var state: RDD[(Boolean, Int, Double)] = null

  private def scale(x: Double, userAvg: Double): Double =
    if x < userAvg then userAvg - 1.0 else if x > userAvg then 5 - userAvg else 1.0

  def init(ratingsRDD: RDD[(Int, Int, Option[Double], Double, Int)]): Unit =
    val movieDeviation = ratingsRDD
      .groupBy(_._1)
      .flatMap(x => x._2.map { y =>
        val rating = y._4
        val userAvg = x._2.map(_._4).sum / x._2.size
        val deviation: Double = (rating - userAvg) / scale(rating, userAvg)

        (y._2, (x._1, deviation))
      })
      .groupByKey()
      .map { x =>
        val avgDeviation: Double = x._2.map(_._2).sum / x._2.size

        (false, x._1, avgDeviation)
      }

    state = ratingsRDD
      .map(x => (x._1, x._4))
      .groupByKey()
      .map(x => (true, x._1, x._2.sum / x._2.size))
      .union(movieDeviation)

    state.persist(MEMORY_AND_DISK)

  def predict(userId: Int, movieId: Int): Double =
    val avgDeviation = state
      .filter(x => !x._1 && x._2 == movieId)
      .map(_._3)
      .reduce(_ + _)

    val avgRating = state
      .filter(x => x._1 && x._2 == userId)
      .map(_._3)
      .reduce(_ + _)

    if (avgRating == 0.0)
      state.filter(_._1).map(_._3).sum() / state.filter(_._1).count()
    else if (avgDeviation == 0.0)
      avgRating
    else
      avgRating + avgDeviation * scale(avgRating + avgDeviation, avgRating)

}
