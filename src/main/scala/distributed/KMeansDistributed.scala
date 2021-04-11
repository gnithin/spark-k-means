package distributed

import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.avg

/**
 * This object runs the KMeans algorithm on a given set of initial centroids in a distributed mode,
 * using the Spark Dataset API. The program returns a list of centroids which are the new, converged
 * centers.
 * */
object KMeansDistributed {

  def main(args: Array[String]): Unit = {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\n <input dir> <output dir>")
      System.exit(1)
    }
    val spark = SparkSession.builder()
      .master("local[4]")
      .appName("KMeans Sequential")
      .getOrCreate()

    logger.info("***************K Means Distributed*************");
    val maxIterations = 10; // to prevent long programs - for convergence
    import spark.implicits._
    // Prepare a list of initial centroids (x,y) coordinates -
    // number of initial centroids in the file depecit the value of K
    // maintaining as a list since k will not be huge and will fit on a single machine.
    var centroidsList = spark.sparkContext.textFile(args(0) + "/centers.txt")
      .map(point => (point.split(",")(0).toDouble, point.split(",")(1).toDouble))
      .collect()
      .toList

    // Prepare a Dataset for the points that need to be assigned to a cluster
    // This dataset is maintained with 2 columns, one for x and the other for y
    val samplePointsDS = spark.sparkContext.textFile(args(0) + "/sample-points.txt")
      .map(point => (point.split(",")(0).toDouble, point.split(",")(1).toDouble))
      .toDS()

    var maxIterationsReached = true
    var prevCentroids = centroidsList

    // running a breakable loop - break the loop if convergence is reached prior to max_iterations
    import scala.util.control.Breaks._
    breakable {
      for (i <- 0 until maxIterations) {
        var updatedCenters = samplePointsDS.map(point => (bestCentroid(point, centroidsList), point._1, point._2))
          .groupBy($"_1") // group all points by the centroid - so all points which have a common 'best centroid' will be grouped together
          .agg(
            avg($"_2").as("avg_x"), // aggregate - average out all the x values for all points belonging to this centroid
            avg($"_3").as("avg_y") // aggregate - average out all the y values for all points belonging to this centroid
          )
        centroidsList = updatedCenters.select("avg_x", "avg_y").as[(Double, Double)].collect().toList
        if (prevCentroids == centroidsList) {
          println("CONVERGENCE REACHED")
          maxIterationsReached = false
          break() // break if convergence reached
        }
        prevCentroids = centroidsList // update previous centroids with the currently found centroids
      }
    }
    if (maxIterationsReached) {
      println("MAX ITERATIONS REACHED")
    }
    // write the new centers back to the file - we use coalesce here so that we get output on a single file
    // this operation is performed only once on a relatively small list
    spark.sparkContext.parallelize(centroidsList).toDF().coalesce(1).write.csv(args(1))
  }

  /**
   * This method returns the best centroid for a given data point. The best centroid for any given
   * data point is the centroid which has the closest distance to the data point. Here the distance
   * from the given data point is computed as the Euclidean distance.
   *
   * @param dataPoint The given data point. In this case it's an XY coordinate.
   * @param centers   The list of available centroids, to which the dataPoint needs to be assigned.
   * @return The best centroid for a given dataPoint as an XY coordinate.
   * */
  def bestCentroid(dataPoint: (Double, Double), centers: List[(Double, Double)]): (Double, Double) = {
    var bestIndex = 0
    var closestDistance = Double.PositiveInfinity

    for (i <- centers.indices) {
      val distance = scala.math.sqrt(scala.math.pow((dataPoint._1 - centers(i)._1), 2) + scala.math.pow((dataPoint._2 - centers(i)._2), 2))
      if (distance < closestDistance) {
        closestDistance = distance
        bestIndex = i;
      }
    }
    centers(bestIndex)
  }

}
