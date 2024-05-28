package app.aggregator

import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK

/**
 * Class for computing the aggregates
 *
 * @param sc The Spark context for the given application
 */
class Aggregator(sc: SparkContext) extends Serializable {

  private var state: RDD[(Int, String, Double, Long, List[String])] = null
  private var partitioner: HashPartitioner = null

  /**
   * Use the initial ratings and titles to compute the average rating for each title.
   * The average rating for unrated titles is 0.0
   *
   * @param ratings The RDD of ratings in the file
   *        format: (user_id: Int, title_id: Int, old_rating: Option[Double], rating: Double, timestamp: Int)
   * @param title   The RDD of titles in the file
   */
  def init(
            ratings: RDD[(Int, Int, Option[Double], Double, Int)],
            title: RDD[(Int, String, List[String])]
          ): Unit =

    partitioner = new HashPartitioner(ratings.partitions.length)

    val titleRatings = ratings
      .map(x => ((x._1, x._2), (x._5, x._4)))
      .reduceByKey(partitioner, (a, b) => if (a._1 > b._1) a else b)
      .map(x => (x._1._2, (x._2._2, 1)))
      .reduceByKey(partitioner, (a, b) => (a._1 + b._1, a._2 + b._2))
      .mapValues(x => (x._1 / x._2, x._2))

    state = title
      .map(t => (t._1, t))
      .leftOuterJoin(titleRatings)
      .map { case (titleId, (title, avgRating)) =>
        val (rating, count) = avgRating.getOrElse((0.0, 0))
        (titleId, title._2, rating, count, title._3)
      }

    state.persist(MEMORY_AND_DISK)

  /**
   * Return pre-computed title-rating pairs.
   *
   * @return The pairs of titles and ratings
   */
  def getResult(): RDD[(String, Double)] =
    state.map(x => (x._2, x._3))

  /**
   * Compute the average rating across all (rated titles) that contain the
   * given keywords.
   *
   * @param keywords A list of keywords. The aggregate is computed across
   *                 titles that contain all the given keywords
   * @return The average rating for the given keywords. Return 0.0 if no
   *         such titles are rated and -1.0 if no such titles exist.
   */
  def getKeywordQueryResult(keywords: List[String]): Double =
    state
      .filter(x => keywords.forall(x._5.contains(_)))
      .map(x => (x._3, 1))
      .fold((0.0, 0))((a, b) => (a._1 + b._1, a._2 + b._2)) match {
      case (_, 0) => -1.0
      case (sum, count) => sum / count
    }

  /**
   * Use the "delta"-ratings to incrementally maintain the aggregate ratings
   *
   *  @param delta Delta ratings that haven't been included previously in aggregates
   *        format: (user_id: Int, title_id: Int, old_rating: Option[Double], rating: Double, timestamp: Int)
   */
  def updateResult(delta_ : Array[(Int, Int, Option[Double], Double, Int)]): Unit =
    val deltaRatings = sc.parallelize(delta_)
      .map(x => ((x._1, x._2), (x._5, x._3, x._4)))
      .reduceByKey((a, b) => if (a._1 > b._1) a else b)
      .map {
        case ((_, title_id), (_, old_rating, rating)) =>
        (title_id, (rating - old_rating.getOrElse(0.0), if (old_rating.isEmpty) 1 else 0))
      }
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))

    state = state
      .map(x => (x._1, x))
      .leftOuterJoin(deltaRatings)
      .map {
        case (titleId, ((_, title, avgRating, nbRatings, keywords), deltaRating)) =>
        val (sum, count) = deltaRating.getOrElse((0.0, 0))
        val newCount = nbRatings + count
        val newRating = if (newCount != 0) (avgRating * nbRatings + sum) / newCount else 0
        (titleId, title, newRating, newCount, keywords)
      }

}
