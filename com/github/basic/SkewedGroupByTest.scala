package com.github.basic

import org.apache.spark.{SparkContext, SparkConf}

import java.util.Random

/**
 * Created by hadoop on 4/17/16.
 */
object SkewedGroupByTest {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SkewedGroupByTest").setMaster("local")
    val sc = new SparkContext(conf)

    var numMappers = if (args.length > 0) args(0).toInt else 2
    var numKVPairs = if (args.length > 1) args(1).toInt else 100
    var valSize = if (args.length > 2) args(2).toInt else 10
    var numReducers = if (args.length > 3) args(3).toInt else numMappers

    val pairs1=sc.parallelize(0 until numMappers,numMappers).flatMap{p =>
      val ranGen=new Random
      numKVPairs=(1.0*(p+1)/numMappers*numKVPairs).toInt

      var arr1=new Array[(Int,Array[Byte])](numKVPairs)
      for(i<-0 until numKVPairs){
        val byteArr=new Array[Byte](valSize)
        ranGen.nextBytes(byteArr)
        arr1(i)=(ranGen.nextInt(Int.MaxValue),byteArr)
      }
      arr1
    }.cache()

    println("RESULT: " + pairs1.collect)
    println(pairs1.count())
    println(pairs1.groupByKey(numReducers).count())
    sc.stop()
  }

}
