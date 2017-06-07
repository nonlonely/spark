package com.github.basic

import java.util.Random

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by hadoop on 4/17/16.
 */
object SimpleSkewedGroupByTest {
  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("SimpleSkewedGroupByTest").setMaster("local")
    var numMappers = if (args.length > 0) args(0).toInt else 2
    var numKVPairs = if (args.length > 1) args(1).toInt else 1000
    var valSize = if (args.length > 2) args(2).toInt else 1000
    var numReducers = if (args.length > 3) args(3).toInt else numMappers
    var ratio = if (args.length > 4) args(4).toInt else 5.0

    val sc = new SparkContext(sparkConf)

    val pairs1 = sc.parallelize(0 until numMappers, numMappers).flatMap { p =>
      val ranGen = new Random
      var result = new Array[(Int, Array[Byte])](numKVPairs)
      for (i <- 0 until numKVPairs) {
        val byteArr = new Array[Byte](valSize)
        ranGen.nextBytes(byteArr)
        val offset = ranGen.nextInt(1000) * numReducers
        if (ranGen.nextDouble < ratio / (numReducers + ratio - 1)) {
          // give ratio times higher chance of generating key 0 (for reducer 0)
          result(i) = (offset, byteArr)
        } else {
          // generate a key for one of the other reducers
          val key = 1 + ranGen.nextInt(numReducers-1) + offset
          result(i) = (key, byteArr)
        }
      }
      result
    }.cache
    // Enforce that everything has been calculated and in cache
    pairs1.count

    println("RESULT: " + pairs1.groupByKey(numReducers).count)
    // Print how many keys each reducer got (for debugging)
    println("RESULT: " + pairs1.collectAsMap)

     println("RESULT: " + pairs1.groupByKey(numReducers)
                            .map{case (k,v) => (k, v.size)}
                            .collectAsMap)

    sc.stop()
  }
}