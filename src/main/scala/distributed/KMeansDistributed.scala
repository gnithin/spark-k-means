package distributed

import org.apache.log4j.LogManager
import org.apache.spark.sql.{Dataset, SparkSession}

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

    val textFile = spark.sparkContext.textFile(args(0))
    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    logger.info("***************K Means Distributed*************");
    val maxIterations = 100; // to prevent long programs - for convergence
    import spark.implicits._
    var centroidsList = spark.sparkContext.textFile(args(0) + "/centers.txt")
      .map(point => (point.split(" ")(0).toFloat, point.split(" ")(1).toFloat))
      .collect()
      .toList

    val samplePointsDS = spark.sparkContext.textFile(args(0) + "/sample-points.txt")
      .map(point => (point.split(" ")(0).toFloat, point.split(" ")(1).toFloat))
      .toDS()

    // currently run for max iterations
    for(i <- 0 until maxIterations) {
    }


    counts.saveAsTextFile(args(1))
  }

  // returns the index of the best centroid
  def bestCentroid(dataPoint: (Float, Float), centers: List[(Float, Float)]): Int = {
    var bestIndex = 0
    var closestDistance = Double.PositiveInfinity

    for (i <- centers.indices) {
      val distance = scala.math.sqrt(scala.math.pow((dataPoint._1 - centers(i)._1),2) + scala.math.pow((dataPoint._2 - centers(i)._2),2))
      if (distance < closestDistance) {
        closestDistance = distance
        bestIndex = i;
      }
    }
    bestIndex
  }

}
