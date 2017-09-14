package com.daxin

import org.apache.spark.Partitioner

/**
  * Created by Daxin on 2017/9/13.
  */
class SortPartitoner(num: Int) extends Partitioner {
  override def numPartitions: Int = num

  val partitionerSize = Integer.MAX_VALUE / num + 1

  override def getPartition(key: Any): Int = {

    val intKey = key.asInstanceOf[Int]

    intKey / partitionerSize
  }
}
