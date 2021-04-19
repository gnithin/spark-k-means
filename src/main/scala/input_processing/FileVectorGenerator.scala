package input_processing

import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object FileVectorGenerator {
  def generate_vector(inputFilePath: String, spark: SparkSession): RDD[(String, Seq[Double])] = {
    // TODO: Does it make sense to read this directly in spark?
    val inputRDD = parse_input(spark.sparkContext, inputFilePath)

    // Convert to a data-frame since ML lib support seems good for it
    val inputDf = spark.createDataFrame(inputRDD).toDF("id", "content")
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
    // Not handling file exception. If it's not there, purposefully fail
    val dataFiles = sc.wholeTextFiles(inputFilePath)
    // TODO: Can the file-name be used in the id?
    dataFiles.flatMap(v =>
      xml.XML.loadString(v._2.trim()) \ "row"
    )
      .map(row =>
        (
          generate_id((row \ "@Id").text, (row \ "@PostTypeId").text),
          (row \ "@Body").text
        )
      )
  }

  def generate_id(id: String, postType: String): String = {
    Array(id, postType).mkString("--")
  }
}
