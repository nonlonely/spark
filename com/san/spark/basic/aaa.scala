package com.san.spark.basic

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by hadoop on 3/23/16.
 */
object aaa {
  def main(args: Array[String]) {
    val logFile = "hdfs://spark-master.dragon.org:8020/user/sparkS/a.csv" // Should be some file on your system
    val conf = new SparkConf().setAppName("aa") //
        .setMaster("spark://spark-master.dragon.org:7077")
        //.setJars(List("/opt/data01/myTes/aaa.jar"))
    //    .setMaster("local")
    val sc = new SparkContext(conf)
    //sc.addJar("/opt/data01/myTes/aaa.jar")
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
    sc.stop()
  }
}
