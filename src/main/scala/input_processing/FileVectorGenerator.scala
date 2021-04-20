package input_processing

import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.{HashingTF, IDF, StopWordsRemover, Tokenizer}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object FileVectorGenerator {
  def generate_vector(inputFilePath: String, spark: SparkSession): RDD[(String, Seq[Double])] = {
    val inputRDD = parse_input(spark.sparkContext, inputFilePath)

    // Convert to a data-frame since ML lib support seems good for it
    val inputDf = spark.createDataFrame(inputRDD).toDF("id", "content")

    // Tokenize the words (This also converts everything to lowercase)
    val tokenizer = new Tokenizer().setInputCol("content").setOutputCol("words")
    val wordsData = tokenizer.transform(inputDf)

    // Remove stop-words
    val remover = new StopWordsRemover()
      .setInputCol("words")
      .setOutputCol("filtered_words")
    val filteredWords = remover.transform(wordsData)

    // TODO: Remove this
//    filteredWords.show()
//    println("*" * 50)

    // TODO: Think about the ideal number of features. It should be the number of words upto a limit (if the number is too big)
    val hashingTF = new HashingTF()
      .setInputCol("filtered_words").setOutputCol("rawFeatures").setNumFeatures(50)
    val featurizedData = hashingTF.transform(filteredWords)

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    val rescaledData = idfModel.transform(featurizedData)

    val idFeatureRdd = rescaledData.rdd.map(row => (
      row.getAs[String]("id"),
      row.getAs[Vector]("features").toArray.toSeq)
    )

    idFeatureRdd
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

  def parse_input(sc: SparkContext, inputFilePath: String): RDD[(String, String)] = {
    /*
    NOTE-1: Purposefully using wholeTextFiles instead of textFiles, since the input
    is XML. In a CSV, since each line is a complete structure, it's fine. In an XML
    that's not the case, however simple the XML maybe. Not resorting to string
    manipulation shenanigans to overcome this, since this is a general solution.
    CAUTION: Remember 2 things -
    - Each individual file shouldn't be too big. Make sure that it can fit in memory
    - Strip all the BOM characters in the XML before processing
     */
    // NOTE-2: Not handling file exception. If it's not there, purposefully fail
    val dataFiles = sc.wholeTextFiles(inputFilePath)

    dataFiles.flatMap(v =>
      (xml.XML.loadString(v._2.trim()) \ "row").map(n => (v._1, n))
    ).map {
      case (filePath, row) =>
        (
          generate_id(
            filePath,
            (row \ "@Id").text,
            (row \ "@PostTypeId").text
          ),
          clean_raw_text((row \ "@Body").text)
        )
    }
  }

  def clean_raw_text(rawText: String): String = {
    // Strip all the tags
    rawText.replaceAll("""(?:<).*?(?:>)""", "")
  }

  def generate_id(filePath: String, id: String, postType: String): String = {
    // Get just the name of the file without the extensions
    val filename = filePath.split("/").last.split('.').head
    Array(filename, id, postType).mkString("--")
  }
}
