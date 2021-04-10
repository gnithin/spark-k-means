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
    // TODO: Add the actual k-means logic here
    println(s"${inputData(10)}")
    (k, 1)
  }
}
