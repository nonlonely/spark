package com.github.basic

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by hadoop on 4/13/16.
 */
object ExceptionHandlingTest {
  def main(args: Array[String]) {
    val conf=new SparkConf().setAppName("ExceptionHandlingTest").setMaster("local[2]")
    val sc=new SparkContext(conf)
    // default ability of parallelism(bing xing ji suan)
    println(sc.defaultParallelism)
    sc.parallelize(0 until sc.defaultParallelism).foreach{i=>
      println(i)
      //math.random 生成指定范围数值的随机数
      if(math.random < 0.75){
        throw new Exception("Testing Exception handing")
      }
    }
    sc.stop()
  }
}
