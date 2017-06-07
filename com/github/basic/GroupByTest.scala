package com.github.basic

import java.util.Random

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by hadoop on 3/27/16.
 */
object GroupByTest {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("GroupBy Test")
    var numMappers = if (args.length > 0) args(0).toInt else 2
    var numKVPairs = if (args.length > 1) args(1).toInt else 1000
    var valSize = if (args.length > 2) args(2).toInt else 1000
    var numReducers = if (args.length > 3) args(3).toInt else numMappers

    val sc = new SparkContext(sparkConf)

    val pairs1 = sc.parallelize(0 until numMappers, numMappers).flatMap { p =>
      val ranGen = new Random
      var arr1 = new Array[(Int, Array[Byte])](numKVPairs)
      for (i <- 0 until numKVPairs) {
        val byteArr = new Array[Byte](valSize)
        //void nextBytes(byte[] bytes)：生成随机字节并将其置于用户提供的 byte 数组中。
        ranGen.nextBytes(byteArr)
        //int nextInt()：返回下一个伪随机数，它是此随机数生成器的序列中均匀分布的 int 值。
        //int nextInt(int n)：返回一个伪随机数，它是取自此随机数生成器序列的、在（包括和指定值（不包括）之间均匀分布的int值。
        arr1(i) = (ranGen.nextInt(Int.MaxValue), byteArr)
      }
      arr1
    }.cache()
    // Enforce that everything has been calculated and in cache
    pairs1.count()

    println(pairs1.groupByKey(numReducers).count())
    //println(pairs1.groupByKey(numReducers).take(1))
    //[Lscala.Tuple2;@6b4fb71e

    sc.stop()
  }
}
