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
    // TODO: Convert into data structure

    val configFiles = sc.textFile(configPath)
    val kValues = configFiles.map(line => Integer.parseInt(line.trim()))

    val res = kValues.map(kMeans)
    res.collect().foreach(r => logger.info(r))

    // TODO: Write output to file
  }

  def kMeans(k: Int): (Int, Int)= {
    // TODO: Add the actual k-means logic here
    (k, 1)
  }
}
