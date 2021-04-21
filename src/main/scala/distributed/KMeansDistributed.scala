package distributed

import input_processing.FileVectorGenerator
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * This object runs the KMeans algorithm on a given set of initial centroids in a distributed mode,
 * using the Spark Dataset API. The program returns a list of centroids which are the new, converged
 * centers.
 * */
object KMeansDistributed {

  def main(args: Array[String]): Unit = {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 4) {
      logger.error("Usage:\n <input_centers> <input_data> <output dir> <master>")
      System.exit(1)
    }
    val spark = SparkSession.builder()
      .master(args(3))
      .appName("KMeans Distributed")
      .getOrCreate()

    logger.info("***************Preparing Data*************");
    val documentVectorRDD = FileVectorGenerator.generate_vector(args(1), spark)

    // Assumption the number of initial centers will be a relatively very small integer
    val initialCenterRowIds = spark.sparkContext.textFile(args(0))
      .map(rowId => rowId.toInt)
      .collect()
      .toList

    // generate internal TF.IDF representation for rows provided by the user to be used as centroids
    val initialCentroids = findTfIdfCentroidRepresentation(initialCenterRowIds, documentVectorRDD)
    logger.info("initial centroids generated are")
    initialCentroids.foreach(println)

    logger.info("***************K Means Distributed*************");
    val resultRDD = distributedKMeans(documentVectorRDD, initialCentroids, spark)

    // write the new centers back to the file - we use coalesce here so that we get output on a single file
    // this operation is performed only once on a relatively small list (based on number of entries)
    resultRDD.coalesce(1).saveAsTextFile(args(2))
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

  def closestCentroid(documentVector: Seq[Double], currentCentroids: Vector[Seq[Double]]): Seq[Double] = {
    var closestDistance = Double.PositiveInfinity
    var bestCentroid = currentCentroids(0) // randomly selected
    currentCentroids.foreach(centroid => {
      val distanceFromCentroid = FileVectorGenerator.calculateDistance(documentVector, centroid)
      if (distanceFromCentroid <= closestDistance) {
        closestDistance = distanceFromCentroid
        bestCentroid = centroid
      }
    })
    bestCentroid
  }

  /**
   * This method returns a Vector of TF.IDF representation for a given row IDs. If a row ID is not
   * found in the input data set, the corresponding output will not be included in the final vector.
   * For instance, if rowIds contain only a single entry and it is invalid ID, then output will be
   * an empty vector.
   *
   * @param rowIds    The rowIds in the dataset, for which Tf.Idf vectors need to be calculated.
   * @param vectorRDD The Pair RDD that contains key value pair of custom generated Row ID & TF.IDF representation of that row.
   * @return A vector containing Tf.IDF representation for each row ID.
   * */
  def findTfIdfCentroidRepresentation(rowIds: List[Int], vectorRDD: RDD[(String, Seq[Double])]): Vector[Seq[Double]] = {
    val vectorTfIdf = vectorRDD.filter(rowIdToTfIdf => rowIds.contains(rowIdToTfIdf._1.split("--")(1).toInt))
      .map(rowIdToTfIdf => rowIdToTfIdf._2)
      .collect().toVector

    vectorTfIdf
  }

  def distributedKMeans(inputData: RDD[(String, Seq[Double])], centroids: Vector[Seq[Double]], sparkSession: SparkSession): RDD[(Seq[Double], Vector[(String, Seq[Double])])] = {
    val maxIterations = 100; // to prevent long programs - for convergence
    var previousCentroids = Vector[Seq[Double]]() // initially empty - so runs at least once
    var currentCentroids = centroids
    var currentIteration = 0;
    var resultRDD: RDD[(Seq[Double], Vector[(String, Seq[Double])])] = sparkSession.sparkContext.emptyRDD

    // run k means till either convergence is reached or max iterations are reached
    while ((previousCentroids != currentCentroids) || currentIteration < maxIterations) {
      currentIteration += 1
      // prepare intermediate result for this iteration
      resultRDD = inputData.map(rowIdToDocVector => (closestCentroid(rowIdToDocVector._2, centroids), Vector((rowIdToDocVector._1, rowIdToDocVector._2))))
        .reduceByKey((accumulator, value) => accumulator ++ value)

      // update the centroids for next iteration based on the prepared results
      previousCentroids = currentCentroids
      currentCentroids = getUpdatedCentroids(resultRDD)
      println("Iteration " + currentIteration + " completed")
    }
    resultRDD
  }

  def getUpdatedCentroids(intermediateResults: RDD[(Seq[Double], Vector[(String, Seq[Double])])]): Vector[Seq[Double]] = {
    var updatedCentroids = Vector[Seq[Double]]()
    val lengthOfDocumentVectors = intermediateResults.first()._1.length
    intermediateResults.map(centroidVectorToDocumentVectors => centroidVectorToDocumentVectors._2)
      .foreach(documentVectors => {
        var avgCentroid = Vector.fill[Double](lengthOfDocumentVectors)(0.0)
        val documentVectorList = documentVectors.map(documentVectors => documentVectors._2)
        val numberOfDocuments = documentVectorList.length

        documentVectorList.foreach(documentVector => avgCentroid.zip(documentVector).map(value => value._1 + value._2))
        avgCentroid = avgCentroid.map(centroidValue => centroidValue / numberOfDocuments)
        updatedCentroids = updatedCentroids :+ avgCentroid
      })
    updatedCentroids
  }
}
