package app.analytics

import org.apache.spark.HashPartitioner
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK

import java.time.{Instant, ZoneId}


class SimpleAnalytics() extends Serializable {

  private var ratingsPartitioner: HashPartitioner = null
  private var moviesPartitioner: HashPartitioner = null

  private var titlesGroupedById: RDD[(Int, Iterable[(Int, String, List[String])])] = null
  private var ratingsGroupedByYearByTitle: RDD[(Int, Map[Int, Iterable[(Int, Int, Option[Double], Double, Int)]])] = null

  def init(
            ratings: RDD[(Int, Int, Option[Double], Double, Int)],
            movie: RDD[(Int, String, List[String])]
          ): Unit = {
    titlesGroupedById = movie.groupBy(_._1).persist(MEMORY_AND_DISK)

    val getYear = (timestamp: Int) =>
      Instant.ofEpochSecond(timestamp).atZone(ZoneId.systemDefault()).getYear

    ratingsGroupedByYearByTitle = ratings
      .map(x => (x._1, x._2, x._3, x._4, getYear(x._5)))
      .groupBy(_._5)
      .mapValues(_.groupBy(_._2))

    ratingsPartitioner = new HashPartitioner(ratings.partitions.length)
    moviesPartitioner = new HashPartitioner(movie.partitions.length)

    ratingsGroupedByYearByTitle = ratingsGroupedByYearByTitle.partitionBy(ratingsPartitioner)
      .persist(MEMORY_AND_DISK)

    titlesGroupedById = titlesGroupedById.partitionBy(moviesPartitioner)
      .persist(MEMORY_AND_DISK)
  }

  def getNumberOfMoviesRatedEachYear: RDD[(Int, Int)] =
    ratingsGroupedByYearByTitle.mapValues(_.size)

  private def getMostRatedEachYear[T](projection: ((Int, String, List[String])) => T): RDD[(Int, T)] =
    ratingsGroupedByYearByTitle
      .mapValues(_.maxBy(x => (x._2.size, x._1)))
      .map(entry => (entry._2._1, entry._1))
      .join(titlesGroupedById)
      .map(x => (x._2._1, projection(x._2._2.head)))

  def getMostRatedMovieEachYear: RDD[(Int, String)] =
    getMostRatedEachYear(_._2)

  def getMostRatedGenreEachYear: RDD[(Int, List[String])] =
    getMostRatedEachYear(_._3)

  // Note: if two genre has the same number of rating, return the first one based on lexicographical sorting on genre.
  def getLeastAndMostRatedGenreAllTime: ((String, Int), (String, Int)) =
    val genreCount = getMostRatedGenreEachYear
      .flatMap(_._2.map((_, 1)))
      .reduceByKey(_ + _)

    val mostRatedGenre = genreCount.reduce((a, b) =>
      if (a._2 > b._2 || (a._2 == b._2 && a._1 < b._1)) a else b
    )

    val leastRatedGenre = genreCount.reduce((a, b) =>
      if (a._2 < b._2 || (a._2 == b._2 && a._1 < b._1)) a else b
    )

    (leastRatedGenre, mostRatedGenre)

  /**
   * Filter the movies RDD having the required genres
   *
   * @param movies         RDD of movies dataset
   * @param requiredGenres RDD of genres to filter movies
   * @return The RDD for the movies which are in the supplied genres
   */
  def getAllMoviesByGenre(movies: RDD[(Int, String, List[String])],
                          requiredGenres: RDD[String]): RDD[String] =
    movies
      .flatMap(x => x._3.map((_, x._2)))
      .groupByKey()
      .join(requiredGenres.map((_, 1)))
      .map(_._2._1)
      .flatMap(identity)
      .distinct()

  /**
   * Filter the movies RDD having the required genres
   * HINT: use the broadcast callback to broadcast requiresGenres to all Spark executors
   *
   * @param movies            RDD of movies dataset
   * @param requiredGenres    List of genres to filter movies
   * @param broadcastCallback Callback function to broadcast variables to all Spark executors
   *                          (https://spark.apache.org/docs/2.4.8/rdd-programming-guide.html#broadcast-variables)
   * @return The RDD for the movies which are in the supplied genres
   */
  def getAllMoviesByGenre_usingBroadcast(movies: RDD[(Int, String, List[String])],
                                         requiredGenres: List[String],
                                         broadcastCallback: List[String] => Broadcast[List[String]]): RDD[String] =
    val broadcastGenres = broadcastCallback(requiredGenres)

    movies
      .flatMap(x => x._3.map((_, x._2)))
      .filter{case (genre, _) => broadcastGenres.value.contains(genre)}
      .groupByKey()
      .flatMap(_._2)
      .distinct()

}

