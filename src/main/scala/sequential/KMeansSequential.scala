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
      val coords = line.split(",").map(v => v.toDouble)
      (coords(0), coords(1))
    }).collect()

    // Broadcast input data
    val broadcastedData = sc.broadcast(inputData)

    // Process config files
    val configFiles = sc.textFile(configPath)
    val kValues = configFiles.map(line => Integer.parseInt(line.trim()))

    // Call kmeans on every entry in the config file
    val kValWithClustersPair = kValues.map(k => (k, kMeans(k, broadcastedData.value)))

    // TODO: Can this formatted to something better?
    // Write output to file
    kValWithClustersPair.saveAsTextFile(args(1))
  }

  def kMeans(k: Int, inputData: Array[(Double, Double)]): Map[(Double, Double), Vector[(Double, Double)]] = {
    // TODO: Can this be moved out somehow?
    val distance: ((Double, Double), (Double, Double)) => Double = (point: (Double, Double), center: (Double, Double)) => {
      // Square root of - (x2 - x1)^2 - (y2 - y1)^2
      Math.pow(
        Math.pow(point._1 - center._1, 2) + Math.pow(point._2 - center._2, 2),
        0.5
      )
    }

    // Get random centroids
    // NOTE: Sampling some entries without any repeats. This will be sufficient if inputData.length
    // is not super huge. Then again it is assumed that it can fit in memory, so we should be fine.
    var centroids = scala.util.Random.shuffle(Vector.range(0, inputData.length))
      .take(k)
      .map(randomIndex => inputData(randomIndex))

    println("Centroids - ")
    centroids.foreach(println)
    println("*****")

    var prevCentroids = Vector[(Double, Double)]()

    var centroidMap: Map[(Double, Double), Vector[(Double, Double)]] = Map()

    // Loop till convergence (centroids do not change)
    while (!(centroids == prevCentroids)) {
      // Reset the map
      centroidMap = Map()

      // Assign each input point to a centroid
      inputData.foreach(point => {
        val minCentroidDistancePair = centroids.map(centroid => {
          (centroid, distance(centroid, point))
        }).minBy(_._2)
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

      println("Map")
      centroidMap.foreach(println)
      println("******")

      // Recalculate centroids
      prevCentroids = centroids
      centroids = Vector[(Double, Double)]()

      centroidMap.foreach(item => {
        val pointsList = item._2
        val pointSize = pointsList.length
        val sumPoints = pointsList.reduce((l, r) => {
          (l._1 + r._1, l._2 + r._2)
        })

        val avgPoints = (sumPoints._1 / pointSize, sumPoints._2 / pointSize)
        centroids = centroids :+ avgPoints
      })

      println("New centroid list")
      centroids.foreach(println)
    }

    // TODO: Return list of centroids and their associated points
    centroidMap
  }
}
