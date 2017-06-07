package com.demo

/**
 * Created by hadoop on 4/5/16.
 */
import java.io._

class Yiibai(val xc: Int, val yc: Int) {
  var x: Int = xc
  var y: Int = yc
  def move(dx: Int, dy: Int) {
    x = x + dx
    y = y + dy
    println ("Yiibai x location : " + x);
    println ("Yiibai y location : " + y);
  }
}

class Location(override val xc: Int, override val yc: Int,
               val zc :Int) extends Yiibai(xc, yc){
  var z: Int = zc

  def move(dx: Int, dy: Int, dz: Int) {
    x = x + dx
    y = y + dy
    z = z + dz
    println ("Yiibai x location : " + x);
    println ("Yiibai y location : " + y);
    println ("Yiibai z location : " + z);
  }
}

object Test {
  def main(args: Array[String]) {
    val loc = new Location(10, 20, 15);

    // Move to a new location
    loc.move(10, 10, 5);
  }
}

