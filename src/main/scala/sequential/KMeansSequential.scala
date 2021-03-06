package sequential

import java.io.File

import input_processing.FileVectorGenerator
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession

import scala.collection.Map

object KMeansSequential {
  val DATA_DIR = "data"
  val CONFIG_DIR = "configuration"
  val THRESHOLD_SOFT_CONVERGENCE_NUM_FEATURES = 49
  val THRESHOLD_SOFT_CONVERGENCE_MAX_DIFF = 0.01

  val MAX_ITERATIONS = 200
  val NUM_PARTITIONS = 4

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

    /*
    Partition the k-values uniformly so that k-means is uniformly distributed across nodes.
    - The zip with index essentially adds an index to every entry
    - Then we flip the pair-rdd, so that the index is key
    - We partition using the index since that will be different for all values (multiple k values can be the same)
    - We use a custom-partitioner since the default partitioner is not always uniform and in some cases produces a skew.
     */
    val kValues = configFiles
      .map(line => {
        // Parse for optional initial-centroids
        // The format is
        // k|id1,id2,id3
        // The "|id1,id2,id3" part is optional
        var initialCentroids = Vector[String]()
        var kVal = 0
        if (line.contains("|")) {
          // Parse the optional lise of centroids
          val components = line.split("""\|""")
          kVal = Integer.parseInt(components.head.trim())
          initialCentroids = components.last
            .split(",")
            .map(c => c.trim())
            .filter(c => c.length > 0)
            .toVector

        } else {
          kVal = Integer.parseInt(line.trim())
        }

        (kVal, initialCentroids)
      })
      .zipWithIndex()
      .map(k => (k._2, k._1))
      .partitionBy(new CustomSequentialKInputPartitioner(NUM_PARTITIONS))

    // Call kmeans on every entry in the config file
    val kValWithClustersPair = kValues.map(kValue => {
      val k = kValue._2._1
      val initialCentroids = kValue._2._2

      val res = kMeans(k, initialCentroids, broadcastedData.value)
      var clusters = List[List[String]]()
      res._2.foreach { case (_, docVectorList) =>
        clusters = clusters :+ docVectorList.map(v => v._1).toList
      }
      (k, clusters.map(
        x => "(" + x.mkString(",") + ")"
      ).mkString(","))
    })

    // Write output to file
    kValWithClustersPair.saveAsTextFile(outputPath)
  }

  def calculateSSE(kMeansMap: Map[Seq[Double], Vector[(String, Seq[Double])]]): Double = {
    /*
    SSE = sum of all (square of distance between document-vector and it's centroid)
     */
    kMeansMap.map {
      case (centroid, documentsList) =>
        documentsList.map {
          case (_, documentVector) =>
            Math.pow(FileVectorGenerator.calculateDistance(centroid, documentVector), 2)
        }.sum
    }.sum
  }

  def areCentroidsEqual(centroids: Vector[Seq[Double]], prevCentroids: Vector[Seq[Double]]): Boolean = {
    val numFeatures = centroids.head.length

    if (numFeatures < THRESHOLD_SOFT_CONVERGENCE_NUM_FEATURES) {
      // println("Performing hard-convergence")
      return centroids == prevCentroids
    }

    // Comparison for convergence for big centroid entries (soft-convergence)
    // println("Performing soft-convergence")
    if (centroids.length != prevCentroids.length) {
      return false
    }

    for (i <- centroids.indices) {
      val lVector = centroids(i)
      val rVector = prevCentroids(i)

      for (j <- lVector.indices) {
        val diff = Math.abs(lVector(j) - rVector(j))
        if (diff > THRESHOLD_SOFT_CONVERGENCE_MAX_DIFF) {
          return false
        }
      }
    }

    true
  }

  def kMeans(k: Int, centroidIds: Vector[String], inputData: Map[String, Seq[Double]]): (Double, Map[Seq[Double], Vector[(String, Seq[Double])]]) = {
    var initialCentroidIds = centroidIds

    // Get random centroids if centroidIds is empty
    if (initialCentroidIds.isEmpty){
      // NOTE: Sampling some entries without any repeats. This will be sufficient if inputData.length
      // is not super huge. Then again it is assumed that it can fit in memory, so we should be fine.
      initialCentroidIds = scala.util.Random.shuffle(Vector.range(0, inputData.size))
        .take(k)
        .map(randomIndex => inputData.keySet.toList(randomIndex))
    }

    // NOTE: Purpose-fully not handling when id is invalid. I'd rather it fail and stop execution than being handled
    var centroids = initialCentroidIds.map(id => inputData(id))
    var prevCentroids = Vector[Seq[Double]]()
    var centroidMap: Map[Seq[Double], Vector[(String, Seq[Double])]] = Map()

    var iterations = 0
    var startTime = System.nanoTime

    // Loop till convergence (centroids do not change or max-iterations reached)
    while (!areCentroidsEqual(centroids, prevCentroids) && iterations < MAX_ITERATIONS) {
      iterations += 1

      // Reset the map
      centroidMap = Map()

      // Assign each input point to a centroid
      inputData.foreach(document => {
        val documentVector = document._2

        val minCentroidDistancePair = centroids.map(centroid => {
          (centroid, FileVectorGenerator.calculateDistance(centroid, documentVector))
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

      val loopDuration = (System.nanoTime - startTime) / 1e9d

      println(s"****** Iteration $iterations ends (Took $loopDuration seconds) for k=$k")

      // Starting the timer since we want to capture the time taken for the while comparison as well!
      startTime = System.nanoTime
    }
    println(s"Num iterations $iterations for k - $k")

    (calculateSSE(centroidMap), centroidMap)
  }
}

