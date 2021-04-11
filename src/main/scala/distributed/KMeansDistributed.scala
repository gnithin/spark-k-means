package distributed

import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.avg

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
    var centroidsList = spark.sparkContext.textFile(args(0) + "/centers.txt")
      .map(point => (point.split(",")(0).toDouble, point.split(",")(1).toDouble))
      .collect()
      .toList

    val samplePointsDS = spark.sparkContext.textFile(args(0) + "/sample-points.txt")
      .map(point => (point.split(",")(0).toDouble, point.split(",")(1).toDouble))
      .toDS()

    // currently run for max iterations
    var i = 0
    var prevCentroids = centroidsList
    for (i <- 0 until maxIterations) {
      var updatedCenters = samplePointsDS.map(point => (bestCentroid(point, centroidsList), point._1, point._2))
        .groupBy($"_1")
        .agg(
          avg($"_2").as("avg_x"),
          avg($"_3").as("avg_y")
        )
      centroidsList = updatedCenters.select("avg_x", "avg_y").as[(Double, Double)].collect().toList
      if (prevCentroids == centroidsList) {
        println("CONVERGENCE REACHED")
      }
    }
    if (i == maxIterations) {
      println("MAX ITERATIONS REACHED")
    }
    spark.sparkContext.parallelize(centroidsList).toDF().coalesce(1).write.csv(args(1))
  }

  // returns the index of the best centroid
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
