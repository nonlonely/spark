package com.github.basic

import org.apache.spark.{SparkContext, SparkConf}


import scala.collection.mutable
import scala.util.Random

/**
 * Created by hadoop on 4/14/16.
 *
 * chuan di bi bao
 */
object SparkTC {

  val numEdges = 9
  val numVertices = 5
  val rand = new Random(42)


  def generateGraph:Seq[(Int,Int)]={
    val edges:mutable.Set[(Int,Int)]=mutable.Set.empty
    while (edges.size<numEdges){
      val from=rand.nextInt(numVertices)
      val to=rand.nextInt(numVertices)
      if(from!=to) edges.+=((from,to))
    }
    edges.toSeq
  }


  /*def generateGraph: Seq[(Int, Int)] = {
    val edges: mutable.Set[(Int, Int)] = mutable.Set.empty
    while (edges.size < numEdges) {
      val from = rand.nextInt(numVertices)
      val to = rand.nextInt(numVertices)
      if (from != to) edges.+=((from, to))
    }
    edges.toSeq
  }*/

  def main(args: Array[String]) {
    val conf=new SparkConf().setAppName("SparkTC").setMaster("local")
    val sc=new SparkContext(conf)

    val slices = if (args.length > 0) args(0).toInt else 2
    var tc = sc.parallelize(generateGraph, slices).cache()


    val edges = tc.map(x => (x._2, x._1))

    var oldCount = 0L
    var nextCount = tc.count()

    do {
      oldCount = nextCount
      // Perform the join, obtaining an RDD of (y, (z, x)) pairs,
      // then project the result to obtain the new (x, z) paths.
      tc = tc.union(tc.join(edges).map(x => (x._2._2, x._2._1))).distinct().cache()
      nextCount = tc.count()
    } while (nextCount != oldCount)


    println("TC has " + tc.count() + " edges.")
    sc.stop()

  }
}
