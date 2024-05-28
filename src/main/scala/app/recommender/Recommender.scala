package app.recommender

import app.recommender.baseline.BaselinePredictor
import app.recommender.collaborativeFiltering.CollaborativeFiltering
import app.recommender.LSH.{LSHIndex, NNLookup}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Class for performing recommendations
 */
class Recommender(sc: SparkContext, index: LSHIndex, ratings: RDD[(Int, Int, Option[Double], Double, Int)]) extends Serializable {
  private val nn_lookup = new NNLookup(index)
  private val collaborativePredictor = new CollaborativeFiltering(10, 0.1, 0, 4)
  collaborativePredictor.init(ratings)

  private val baselinePredictor = new BaselinePredictor()
  baselinePredictor.init(ratings)

  private def recommend(userId: Int, genre: List[String], K: Int, predictor: (Int, Int) => Double): List[(Int, Double)] = 
    val ratedMovies = sc
      .broadcast(ratings
        .filter(_._1 == userId)
        .map(_._2)
        .collect().toSet
      )

    nn_lookup
      .lookup(
        sc
          .parallelize(List(genre))
      )
      .map(_._2)
      .flatMap(_.map(_._1))
      .filter(!ratedMovies.value.contains(_))
      .collect().toList
      .map(x => (x, predictor(userId, x)))
      .sortBy(x => -x._2)
      .take(K)
  
  /**
   * Returns the top K recommendations for movies similar to the List of genres
   * for userID using the BaseLinePredictor
   */
  def recommendBaseline(userId: Int, genre: List[String], K: Int): List[(Int, Double)] =
    recommend(userId, genre, K, baselinePredictor.predict)

  /**
   * The same as recommendBaseline, but using the CollaborativeFiltering predictor
   */
  def recommendCollaborative(userId: Int, genre: List[String], K: Int): List[(Int, Double)] =
    recommend(userId, genre, K, collaborativePredictor.predict)
}
