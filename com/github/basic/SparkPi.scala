package com.github.basic

import scala.math.random


import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by hadoop on 4/14/16.
 */
object SparkPi {
  def main(args: Array[String]) {

    val conf=new SparkConf().setAppName("Sparkpi").setMaster("local[2]")
    val sc=new SparkContext(conf)

    val slices=if(args.length>0) args(0).toInt else 2
    val n=math.min(100000L * slices,Int.MaxValue).toInt

    val count = sc.parallelize(1 until n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x*x + y*y < 1) 1 else 0
    }.reduce(_ + _)

    println("Pi is roughly " + 4.0 * count / n)
    sc.stop()
  }
}
