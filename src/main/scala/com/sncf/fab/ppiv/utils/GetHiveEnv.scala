package com.sncf.fab.ppiv.utils

import com.sncf.fab.ppiv.utils.AppConf.{HOST, PORT, PPIV, SPARK_MASTER}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ELFI03951 on 12/07/2017.
  */
object GetHiveEnv {
  def getHiveContext(sc : SparkContext) : HiveContext = {
  val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    hiveContext.setConf("hive.exec.dynamic.partition", "true")
    hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    hiveContext
  }

}
