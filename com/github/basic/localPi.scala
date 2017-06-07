package com.github.basic


import org.apache.spark.{SparkContext, SparkConf}

import scala.math.random
/**
 * Created by hadoop on 4/18/16.
 */
object localPi {
  def main(args: Array[String]): Unit ={
    /*
    var count=0
    for(i<-0 until 1000){
      var x=random * 2 - 1
      var y=random * 2 - 1
      if(x*x+y*y<1) count += 1
    }
    println("Pi is roughly " + 4 * count / 1000.0)
    */

    val conf=new SparkConf().setAppName("pi").setMaster("local")
    val sc=new SparkContext(conf)

    val num = if(args.length>1) args(0).toInt else 1000
    val slice=if(args.length>2) args(2).toInt else 3
    var count=sc.parallelize(1 until num,slice).map{i=>
      var x=random * 2 - 1
      var y=random * 2 - 1
      if(x*x+y*y<1) 1 else 0
    }.reduce(_+_)

    println("Pi is roughly " + 4 * count / num.toDouble)
    
    sc.stop()
  }
}
