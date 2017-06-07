package com.github.graph

import org.apache.spark.graphx.GraphLoader
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by hadoop on 4/10/16.
 */
object GraphPageRunk {
  def main (args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("GraphPageRank").setMaster("local")
    val sc = new SparkContext(sparkConf)
    //val file = sc.textFile("hdfs://spark-master.dragon.org:8020/user/web-Google.txt")
    val graph=GraphLoader.edgeListFile(sc,"hdfs://spark-master.dragon.org:8020/user/web-Google.txt")
    val grapgVCount=graph.vertices.count()
    val grapgECount=graph.edges.count
    val VCTop10=graph.vertices.take(10)
    val ECTop10=graph.edges.take(10)
    println(grapgECount)
    println(VCTop10)
    val graphMapV=graph.mapVertices((id,attr)=>attr.toInt*2)
//  val graphMapV=graph.mapVertices((_,attr)=>attr*2)
    val VCMapTop10=graphMapV.vertices.take(10)
    val graphMapE=graph.mapEdges(e=>e.attr.toInt*3)
    val graphMapTrip=graph.mapTriplets(et=>et.dstAttr.toInt*2+et.srcAttr.toInt*3)
    val subGRaph=graph.subgraph(et=>et.dstAttr>et.srcAttr)
    val subVCTop10=subGRaph.vertices.take(10)
    val outDeg=subGRaph.outDegrees
    val tmp=subGRaph.joinVertices[Int](outDeg)((_,_,optdeg)=>optdeg)
    val tmpe=subGRaph.outerJoinVertices[Int,Int](outDeg)((_,_,optdeg)=>optdeg.getOrElse(0))
    //val pagerunk=graph.pageRank(0.01).vertices
    //println(pagerunk)

  }
}
