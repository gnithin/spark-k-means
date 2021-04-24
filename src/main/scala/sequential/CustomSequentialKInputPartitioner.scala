package sequential

import org.apache.spark.Partitioner

/*
A custom partitioner that is the simplest form of a hash-partitioner
 */
class CustomSequentialKInputPartitioner(numberOfPartitions: Int) extends Partitioner {
  override def numPartitions: Int = numberOfPartitions

  override def getPartition(key: Any): Int = {
    // Perform a simple hasdistribution based on the key
    val k = key.asInstanceOf[Long]
    (k % numberOfPartitions).asInstanceOf[Int]
  }
}
