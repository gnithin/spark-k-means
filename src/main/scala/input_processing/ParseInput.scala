package input_processing

import org.apache.log4j.LogManager

object ParseInput {
  def main(args: Array[String]): Unit = {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger

    // Right now, only processing one input-file file at a time
    if (args.length != 2) {
      logger.error("Usage:\n <input-file-path> <output-dir-path>")
      System.exit(1)
    }
    print(args(0))
  }
}
