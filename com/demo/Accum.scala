package com.demo

/**
 * Created by hadoop on 4/5/16.
 */

 class Accum(xi : Int, yi : Int) {

  var x:Int=xi
  var y:Int=yi

  def getX():Int ={x}
  def getY():Int={y}

  def set(xi:Int,yi:Int){
    x=xi
    y=yi
  }

  def sumXY(x:Int,y:Int):Int={ x+y }
  def plusXY(x:Int,y:Int):Int={ x-y }
}
/*
object Accum {
  def main(args: Array[String]) {
    var ac = new Accum1(10,12)
    ac.set(12,13)
    // Move to a new location
    println(ac.getX())
  }

}
*/