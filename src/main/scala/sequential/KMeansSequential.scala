package sequential

import java.io.File

import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}

object KMeansSequential {
  val DATA_DIR = "data";
  val CONFIG_DIR = "configuration";

  def main(args: Array[String]): Unit = {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\n <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("KMeans Sequential").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val inputPath = args(0)
    val configPath = inputPath + File.separator + CONFIG_DIR
    val dataPath = inputPath + File.separator + DATA_DIR

    val dataFiles = sc.textFile(dataPath)

    // Convert input data into a list
    val inputData = dataFiles.map(line => {
      val coords = line.split(",").map(v => Integer.parseInt(v))
      (coords(0), coords(1))
    }).collect()

    // Broadcast input data
    val broadcastedData = sc.broadcast(inputData)

    // Process config files
    val configFiles = sc.textFile(configPath)
    val kValues = configFiles.map(line => Integer.parseInt(line.trim()))

    // Call kmeans on every entry in the config file
    val res = kValues.map(k => kMeans(k, broadcastedData.value))
    res.collect().foreach(r => logger.info(r))

    // TODO: Write output to file
  }

  def kMeans(k: Int, inputData: Array[(Int, Int)]): (Int, Int) = {
    val distance: ((Int, Int), (Int, Int)) => Double = (point: (Int, Int), center: (Int, Int)) => {
      // Square root of - (x2 - x1)^2 - (y2 - y1)^2
      Math.pow(
        Math.pow(point._1 - center._1, 2) + Math.pow(point._2 - center._2, 2),
        0.5
      )
    }

    val random = scala.util.Random
    var centroids = Array.range(0, k).map(_ => {
      inputData(random.nextInt(inputData.length))
    })
    var prevCentroids = Array[(Int, Int)]()
    var debugIsDone = false
    var centroidMap: Map[(Int, Int), Vector[(Int, Int)]] = Map()

    // Loop till convergence (centroids do not change)
    while (!debugIsDone && !centroids.sameElements(prevCentroids)) {

      // Assign points into clusters

      // Reset the map
      centroidMap = Map()

      inputData.foreach(point => {
        val minCentroidDistancePair = centroids.map(centroid => {
          (centroid, distance(centroid, point))
        }).minBy(_._2)

        println(minCentroidDistancePair)

        val minCentroid = minCentroidDistancePair._1
        if (centroidMap.contains(minCentroid)) {
          val clusterList = centroidMap(minCentroid)
          val newClusterList = clusterList :+ point
          centroidMap += (minCentroid -> newClusterList)
        } else {
          val newClusterList = Vector(point)
          centroidMap += (minCentroid -> newClusterList)
        }
      })

      // TODO: Recalculate centroids
      prevCentroids = centroids
      // TODO: Remove this break
      debugIsDone = true
    }

    println(s"${inputData(10)}")

    // Return list of centroids and their associated points
    (k, 1)
  }
}
