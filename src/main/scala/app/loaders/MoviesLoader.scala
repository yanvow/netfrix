package app.loaders

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import java.io.File

/**
 * Helper class for loading the input
 *
 * @param sc   The Spark context for the given application
 * @param path The path for the input file
 */
class MoviesLoader(sc: SparkContext, path: String) extends Serializable {

  /**
   * Read the title file in the given path and convert it into an RDD
   *
   * @return The RDD for the given titles
   */
  def load(): RDD[(Int, String, List[String])] =
    val filePath = getClass.getResource(path).getPath
    val lines = sc.textFile(filePath)

    lines.map { line =>
      val fields = line.replaceAll("\"", "").split('|')
      val movieId = fields(0).toInt
      val movieTitle = fields(1)
      val movieKeywords = List.range(2, fields.length).map(fields(_))

      (movieId, movieTitle, movieKeywords)
    }.cache()
}

