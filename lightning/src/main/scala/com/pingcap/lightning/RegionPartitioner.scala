package com.pingcap.lightning

import org.apache.spark.Partitioner
import org.tikv.common.key.Key
import org.tikv.common.region.TiRegion

class RegionPartitioner(regions: Array[TiRegion]) extends Partitioner with Serializable {
  override def numPartitions: Int = {
    regions.length + 1
  }

  override def getPartition(keyAny: Any): Int = {
    val key = keyAny.asInstanceOf[Array[Byte]]
    val rawKey = Key.toRawKey(key)
    var i = 0
    var j = regions.length
    while (i < j) {
      val m = (i + j) / 2
      if (Key.toRawKey(regions(m).getEndKey).compareTo(rawKey) > 0) {
        j = m
      } else {
        i = m + 1
      }
    }
    i
  }

  def getPartitionRegion(i: Int): TiRegion = {
    if (i < regions.length) {
      regions(i)
    } else {
      null
    }
  }
}
