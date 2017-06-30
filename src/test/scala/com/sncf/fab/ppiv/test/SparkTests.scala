package com.sncf.fab.ppiv.test

/**
  * Created by ELFI03951 on 30/06/2017.
  */
import com.sncf.fab.ppiv.utils.AppConf.{PPIV, SPARK_MASTER}
import org.specs2.Specification
import org.apache.spark.{SparkConf, SparkContext}

trait SparkTests extends Specification{
  var sc: SparkContext = _
  def runTest[A](name: String)(body: => A): A = {
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.hostPort")
    val sparkConf = new SparkConf()
      .setAppName(PPIV)
      .setMaster(SPARK_MASTER)

    sc = new SparkContext(sparkConf)
    try{
      println("Running test " + name)
      body
    }
    finally {
      sc.stop
      System.clearProperty("spark.driver.port")
      System.clearProperty("spark.hostPort")
      sc = null
    }
  }
}