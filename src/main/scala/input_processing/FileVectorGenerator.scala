package input_processing

import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object FileVectorGenerator {
  def generate_vector(inputFilePath: String, spark: SparkSession): RDD[(String, Seq[Double])] = {
    val inputRDD = parse_input(spark.sparkContext, inputFilePath)

    // Convert to a data-frame since ML lib support seems good for it
    val inputDf = spark.createDataFrame(inputRDD).toDF("id", "content")

    // TODO: Remove this
    inputDf.show()
    println("*" * 50)

    val tokenizer = new Tokenizer().setInputCol("content").setOutputCol("words")
    val wordsData = tokenizer.transform(inputDf)

    // TODO: Remove stop-words

    // TODO: What should the 50 be replaced with? It should be a big number, but what?
    val hashingTF = new HashingTF()
      .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(50)
    val featurizedData = hashingTF.transform(wordsData)

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    val rescaledData = idfModel.transform(featurizedData)

    val idFeatureRdd = rescaledData.rdd.map(row => (
      row.getAs[String]("id"),
      row.getAs[Vector]("features").toArray.toSeq)
    )

    idFeatureRdd
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
