package sequential

import java.io.File

import input_processing.FileVectorGenerator
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession

import scala.collection.Map

object KMeansSequential {
  val DATA_DIR = "data";
  val CONFIG_DIR = "configuration";

  // TODO: Think about the correct entry
  val MAX_ITERATIONS = 10

  def main(args: Array[String]): Unit = {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\n <input dir> <output dir>")
      System.exit(1)
    }

    val inputPath = args(0)
    val configPath = inputPath + File.separator + CONFIG_DIR
    val dataPath = inputPath + File.separator + DATA_DIR
    val outputPath = args(1)

    val spark = SparkSession.builder()
      .master("local[4]")
      .appName("KMeans Sequential")
      .getOrCreate()

    val vectorRdd = FileVectorGenerator.generate_vector(dataPath, spark)
    val sc = spark.sparkContext

    val inputData = vectorRdd.collectAsMap()

    // Broadcast input data
    val broadcastedData = sc.broadcast(inputData)

    // Process config files
    val configFiles = sc.textFile(configPath)
    val kValues = configFiles.map(line => Integer.parseInt(line.trim()))

    // Call kmeans on every entry in the config file
    val kValWithClustersPair = kValues.map(k => (k, kMeans(k, broadcastedData.value)))

    // Write output to file
    kValWithClustersPair.saveAsTextFile(outputPath)
  }

  def calculateDistance(point: Seq[Double], center: Seq[Double]): Double = {
    // Cosine similarity
    // cos(theta) = A.B / |A|*|B|
    val dotProduct = point.zip(center).
      map(entry => entry._1 * entry._2).sum

    val pointMagnitude = Math.pow(point.map(e => Math.pow(e, 2)).sum, 0.5)
    val centerMagnitude = Math.pow(center.map(e => Math.pow(e, 2)).sum, 0.5)
    val magnitude = pointMagnitude * centerMagnitude

    // Bigger the cos value, more similar they are. So just inverting this
    // so that it fits into the distance idea, where a smaller distance would mean
    // they are closer together.
    1.0 - (dotProduct / magnitude)
  }

  def calculateSSE(kMeansMap: Map[Seq[Double], Vector[(String, Seq[Double])]]): Double = {
    /*
    SSE = sum of all (square of distance between document-vector and it's centroid)
     */
    kMeansMap.map {
      case (centroid, documentsList) =>
        documentsList.map {
          case (_, documentVector) =>
            Math.pow(calculateDistance(centroid, documentVector), 2)
        }.sum
    }.sum
  }

  def kMeans(k: Int, inputData: Map[String, Seq[Double]]): (Double, Map[Seq[Double], Vector[(String, Seq[Double])]]) = {
    // Get random centroids
    // NOTE: Sampling some entries without any repeats. This will be sufficient if inputData.length
    // is not super huge. Then again it is assumed that it can fit in memory, so we should be fine.
    var centroids = scala.util.Random.shuffle(Vector.range(0, inputData.size))
      .take(k)
      .map(randomIndex => {
        val randomKey = inputData.keySet.toList(randomIndex)
        inputData(randomKey)
      })

    // TODO: Remove this at the end
    println("Centroids - ")
    centroids.foreach(println)
    println("*****")

    var prevCentroids = Vector[Seq[Double]]()

    var centroidMap: Map[Seq[Double], Vector[(String, Seq[Double])]] = Map()

    var iterations = 0

    // TODO: Fix the comparison for convergence
    // Loop till convergence (centroids do not change or max-iterations reached)
    while (!(centroids == prevCentroids) && iterations < MAX_ITERATIONS) {
      iterations += 1

      // Reset the map
      centroidMap = Map()

      // Assign each input point to a centroid
      inputData.foreach(document => {
        val documentVector = document._2

        val minCentroidDistancePair = centroids.map(centroid => {
          (centroid, calculateDistance(centroid, documentVector))
        }).minBy(_._2)
        val minCentroid = minCentroidDistancePair._1

        if (centroidMap.contains(minCentroid)) {
          val clusterList = centroidMap(minCentroid)
          val newClusterList = clusterList :+ document
          centroidMap += (minCentroid -> newClusterList)

        } else {
          val newClusterList = Vector(document)
          centroidMap += (minCentroid -> newClusterList)
        }
      })

      // TODO: Remove this at the end
      println("----- Map")
      centroidMap.foreach(println)

      // Recalculate centroids
      prevCentroids = centroids
      centroids = Vector[Seq[Double]]()
      val vectorSize = prevCentroids.head.length

      centroidMap.foreach {
        case (centroidKey, documentsList) => {
          // Since the documentsList are of equal size, we can avg them out
          var avgCentroid = Vector.fill[Double](vectorSize)(0.0)
          val documentVectorsList = documentsList.map(d => d._2)
          val numberOfDocuments = documentVectorsList.length

          documentVectorsList.foreach(documentVector => {
            avgCentroid = avgCentroid.zip(documentVector).map(v => v._1 + v._2)
          })
          avgCentroid = avgCentroid.map(e => e / numberOfDocuments)
          centroids = centroids :+ avgCentroid
        }
      }

      // TODO: Remove this at the end
      println("------ New centroid list")
      centroids.foreach(println)
      println("****** Iteration ends")
    }

    println(s"Num iterations $iterations for k - $k")

    (calculateSSE(centroidMap), centroidMap)
  }
}
