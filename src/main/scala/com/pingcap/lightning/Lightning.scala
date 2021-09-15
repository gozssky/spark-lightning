package com.pingcap.lightning

import org.apache.spark.SparkConf

object Lightning {
  def main(args: Array[String]): Unit = {
    val conf = new Config()
      //      .setSourceDir("/home/xyj/test-data")
      .setSourceDir("s3://alice?access-key=minioadmin&secret-access-key=minioadmin&endpoint=http://192.168.49.1:9000")
      .setTiDBHost("192.168.49.1").setTiDBPort(4000)
      .setTiDBStatusHost("192.168.49.1").setTiDBStatusPort(10080)
      .setPDAddr("192.168.49.1:2379")
    val sparkConf = new SparkConf()
      .setIfMissing("spark.master", "local[*]")
      .setIfMissing("spark.app.name", "tidb-lightning")
    val controller = new RestoreController(conf, sparkConf)
    controller.restoreAll()
  }
}
