package distributed

import input_processing.FileVectorGenerator
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import sequential.KMeansSequential.areCentroidsEqual

/**
 * This object runs the KMeans algorithm on a given set of initial centroids in a distributed mode,
 * using the Spark Dataset API. The program returns a list of centroids which are the new, converged
 * centers.
 * */
object KMeansDistributed {

  val MAX_ITERATIONS = 100 // to prevent long programs - for convergence

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
    // try to persist this so that its avoided being sent from all nodes in each kmeans iteration
    documentVectorRDD.cache();

    // Assumption the number of initial centers will be a relatively very small integer
    val initialCenterRowIds = spark.sparkContext.textFile(args(0)).collect().toList

    // generate internal TF.IDF representation for rows provided by the user to be used as centroids
    val initialCentroids = findTfIdfCentroidRepresentation(initialCenterRowIds, documentVectorRDD)
    logger.info("initial centroids generated are")
    initialCentroids.foreach(println)

    logger.info("***************K Means Distributed*************");
    val resultRDD = distributedKMeans(documentVectorRDD, initialCentroids, spark)

    // write the new centers back to the file
    resultRDD.saveAsTextFile(args(2))
  }

  /**
   * This method returns the closes centroid for a given document vector. The best centroid for any given
   * document vector is the centroid that is most similar to the passed document vector. This similarity
   * is computed using the cosine similarity function.
   *
   * @param documentVector   The given document vector. Its a text document represented as a sequence of numbers computed using tf.Idf of the words.
   * @param currentCentroids The list of available centroids, to which the document vector needs to be assigned.
   * @return The best centroid for a given document vector
   * */
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
  def findTfIdfCentroidRepresentation(rowIds: List[String], vectorRDD: RDD[(String, Seq[Double])]): Vector[Seq[Double]] = {
    // IDs in RDD are in the following format - movie_posts--10127--2
    val vectorTfIdf = vectorRDD.filter(rowIdToTfIdf => rowIds.contains(rowIdToTfIdf._1))
      .map(rowIdToTfIdf => rowIdToTfIdf._2)
      .collect().toVector
    vectorTfIdf
  }

  /**
   * The actual algorithm that runs iterative k-means on the input data. The iterations continue till either the MAX_ITERATIONS are reached
   * or the centroids converge. The convergence of centroid is governed by another method that is defined in KMeansSequential file.
   *
   * @param inputData    The input pair RDD that contains the documents as pairs of document ID and their tf.idf vectors.
   * @param centroids    The initial centroids to which these documents need te assigned.
   * @param sparkSession The current spark session. This parameter is used to access the Spark context.
   * @return A pair RDD of document vectors of centroids against a list of documents that 'belong' to those centroids.
   * */
  def distributedKMeans(inputData: RDD[(String, Seq[Double])], centroids: Vector[Seq[Double]], sparkSession: SparkSession): RDD[(Seq[Double], Vector[(String, Seq[Double])])] = {
    var previousCentroids = Vector[Seq[Double]]() // initially empty - so runs at least once
    var currentCentroids = centroids
    var currentIteration = 0;
    var resultRDD: RDD[(Seq[Double], Vector[(String, Seq[Double])])] = sparkSession.sparkContext.emptyRDD

    // run k means till either convergence is reached or max iterations are reached
    while (!areCentroidsEqual(currentCentroids, previousCentroids) && currentIteration < MAX_ITERATIONS) {
      val startTime = System.nanoTime
      currentIteration += 1
      // prepare intermediate result for this iteration
      resultRDD = inputData.map(rowIdToDocVector => (closestCentroid(rowIdToDocVector._2, currentCentroids), Vector((rowIdToDocVector._1, rowIdToDocVector._2))))
        .reduceByKey((accumulator, value) => accumulator ++ value)

      // update the centroids for next iteration based on the prepared results
      previousCentroids = currentCentroids
      currentCentroids = getUpdatedCentroids(resultRDD)
      val iterationTime = (System.nanoTime - startTime) / 1e9d
      println(s"Iteration ${currentIteration} completed, took $iterationTime seconds")
    }
    resultRDD
  }

  /**
   * This method is used to get the updated centroids for use in the next iteration of the kmeans.
   * The new centroids are calculated by computing simple avergaes of all features of the document
   * vectors in the current centroid.
   *
   * @param intermediateResults A mapping of document vectors of current centroids and the documents
   *                            clustered with those centroids.
   * @return A Vector containing updated centroids to which the documents must be re-assigned in the
   *         next iteration.
   * */
  def getUpdatedCentroids(intermediateResults: RDD[(Seq[Double], Vector[(String, Seq[Double])])]): Vector[Seq[Double]] = {
    var updatedCentroids = Vector[Seq[Double]]()
    val lengthOfDocumentVectors = intermediateResults.first()._1.length
    intermediateResults.map(centroidVectorToDocumentVectors => centroidVectorToDocumentVectors._2)
      .collect()
      .foreach(documentVectors => {
        var avgCentroid = Vector.fill[Double](lengthOfDocumentVectors)(0.0)
        val documentVectorList = documentVectors.map(documentVectors => documentVectors._2)
        val numberOfDocuments = documentVectorList.length

        documentVectorList.foreach(documentVector => {
          avgCentroid = avgCentroid.zip(documentVector).map(value => value._1 + value._2)
        })
        avgCentroid = avgCentroid.map(centroidValue => centroidValue / numberOfDocuments)
        updatedCentroids = updatedCentroids :+ avgCentroid
      })
    updatedCentroids
  }
}
