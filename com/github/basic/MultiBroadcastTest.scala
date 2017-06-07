package com.github.basic

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by hadoop on 4/17/16.
 */
object MultiBroadcastTest {
  def main (args: Array[String]) {
    val conf=new SparkConf().setAppName("MultiBroadcaseTest").setMaster("local")
    val sc=new SparkContext(conf)

    val num1=if(args.length>1) args(0).toInt else 2
    val num2=if(args.length>2) args(1).toInt else 100

    val arr1=new Array[Int](num2)
    for(i<-0 until arr1.length){
      arr1(i)=i
    }

    val arr2=new Array[Int](num2)
    for(i<-0 until arr2.length){
      arr1(i)=i
    }

    val bro1=sc.broadcast(arr1)
    val bro2=sc.broadcast(arr2)

    val ObsertRDD:RDD[(Int,Int)]=sc.parallelize(1 to 10,num1).map{_=>
      (bro1.value.length,bro2.value.length)
    }

    ObsertRDD.collect().foreach(i=>println(i))

    sc.stop()
  }
}
