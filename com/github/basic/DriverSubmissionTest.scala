package com.github

import scala.collection.JavaConverters._


/**
 * Created by hadoop on 3/27/16.
 */

object DriverSubmissionTest {
  def main(args: Array[String]) {
    if (args.length < 1) {
      println("Usage: DriverSubmissionTest <seconds-to-sleep>")
      System.exit(0)
    }
    val numSecondsToSleep = args(0).toInt

    val env = System.getenv()
   // val properties = Utils.getSystemProperties()

    println("Environment variables containing SPARK_TEST:")
    env.asScala.filter{case(k, _) => k.toString.contains("SCALA")}.foreach(println)

    //println("System properties containing spark.test:")
   // properties.filter { case (k, _) => k.toString.contains("spark.test") }.foreach(println)

    for (i <- 1 until numSecondsToSleep) {
      println(s"Alive for $i out of $numSecondsToSleep seconds")
      Thread.sleep(1000)
    }
  }
}
