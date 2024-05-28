package app.loaders

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import java.io.File

/**
 * Helper class for loading the input
 *
 * @param sc The Spark context for the given application
 * @param path The path for the input file
 */
class RatingsLoader(sc : SparkContext, path : String) extends Serializable {

  /**
   * Read the rating file in the given path and convert it into an RDD
   *
   * @return The RDD for the given ratings
   */
  def load() : RDD[(Int, Int, Option[Double], Double, Int)] =
    val filePath = getClass.getResource(path).getPath
    val lines = sc.textFile(filePath)

    lines.map { line =>
      val fields = line.split('|')
      val userId = fields(0).toInt
      val movieId = fields(1).toInt
      val rating = fields(2).toDouble
      val timestamp = fields(3).toInt

      (userId, movieId, Option.empty[Double], rating, timestamp)
    }.cache()
}