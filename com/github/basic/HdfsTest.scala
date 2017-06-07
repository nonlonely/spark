package com.github.basic

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by hadoop on 3/24/16.
 */
object HdfsTest {
  /** Usage: HdfsTest [file] */
  def main(args: Array[String]) {
    /*
    if (args.length < 1) {
      System.err.println("Usage: HdfsTest <file>")
      System.exit(1)
    }
    */
    val sparkConf = new SparkConf().setAppName("HdfsTest").setMaster("local")
    val sc = new SparkContext(sparkConf)
    //val file = sc.textFile(args(0))
    val file = sc.textFile("hdfs://spark-master.dragon.org:8020/user/sparkS/a.csv")
    val mapped = file.map(s => s.length).cache()

    for (iter <- 1 to 10) {
      val start = System.currentTimeMillis()
      for (x <- mapped) {
        println("Iteration1 " + x + " took1 " + (x+2))
      }
      val end = System.currentTimeMillis()
      println("Iteration " + iter + " took " + (end-start) + " ms")
    }
    sc.stop()
  }
}
