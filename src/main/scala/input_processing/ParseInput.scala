package input_processing

import org.apache.log4j.LogManager
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.SparkSession

object ParseInput {
  def main(args: Array[String]): Unit = {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger

    // Right now, only processing one input-file file at a time
    if (args.length != 2) {
      logger.error("Usage:\n <input-file-path> <output-dir-path>")
      System.exit(1)
    }

    val inputFilePath = args(0)
    val outputDirPath = args(1)

    // TODO: Does it make sense to read this directly in spark?
    val inputSeq = parse_input(inputFilePath)
    val spark = SparkSession.builder()
      .master("local[4]")
      .appName("parse-input")
      .getOrCreate()

    // Convert to a data-frame
    val inputDf = spark.createDataFrame(inputSeq).toDF("id", "content", "tags")
    val tokenizer = new Tokenizer().setInputCol("content").setOutputCol("words")
    val wordsData = tokenizer.transform(inputDf)

    // TODO: What should the 50 be replaced with? It should be a big number, but what?
    val hashingTF = new HashingTF()
      .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(50)
    val featurizedData = hashingTF.transform(wordsData)

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    val rescaledData = idfModel.transform(featurizedData)

    val idFeatureRdd = rescaledData.rdd.map(row => (row.getAs[String]("id"), row.getAs[Vector]("features").toArray.toSeq))
    idFeatureRdd.collect().foreach(println)

    // TODO: Think about whether this is really necessary
    idFeatureRdd.saveAsTextFile(outputDirPath + "/rdd_output")
  }

  def parse_input(inputFilePath: String): Seq[(String, String, String)] = {
    // Not handling file exception. If it's not there, purposefully fail
    val inputXml = xml.XML.load(inputFilePath)
    val rows = inputXml \ "row"
    rows.map(row =>
      (
        generate_id((row \ "@Id").text, (row \ "@PostTypeId").text),
        (row \ "@Body").text,
        (row \ "@Tags").text
      )
    )
  }

  def generate_id(id: String, postType: String): String = {
    Array(id, postType).mkString("--")
  }
}
