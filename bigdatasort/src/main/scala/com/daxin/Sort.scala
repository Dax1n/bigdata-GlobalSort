package com.daxin

import org.apache.spark.{SparkContext, SparkConf}

/**
  * @author ${user.name}
  *
  *         <br>
  *         Spark 默认reduceByKey使用的是HashPartitioner
  *         <br>
  *         还有一个Partitioner的分区器：RangePartitioner
  *
  */
object Sort {


  def main(args: Array[String]) {

    val conf = new SparkConf()

    val sc = new SparkContext(conf)
    //Persist this RDD with the default storage level (MEMORY_ONLY).
    val numbers = sc.textFile("/random.txt").flatMap(_.split(" ")).map(x => (x.toInt, 1)).cache()
    val result = numbers.repartitionAndSortWithinPartitions(new SortPartitoner(numbers.partitions.length)).map(x => x._1)



    numbers.sortByKey()

    result.saveAsTextFile("/bigdatasort")
    sc.stop()
  }

}
