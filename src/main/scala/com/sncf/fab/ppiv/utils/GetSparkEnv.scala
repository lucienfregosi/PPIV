package com.sncf.fab.ppiv.utils

import com.sncf.fab.ppiv.utils.AppConf.{HOST, PORT, PPIV, SPARK_MASTER}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ELFI03951 on 12/07/2017.
  */
object GetSparkEnv {
  def getSparkConf() : SparkConf = {
    new SparkConf()
      .setAppName(PPIV)
     // .setMaster(SPARK_MASTER)
      .set("es.nodes", HOST)
      .set("es.port", PORT)
      .set("es.index.auto.create", "true")
  }
  @transient val sparkConf = getSparkConf()

  @transient val sc = new SparkContext(sparkConf)
  @transient val sqlContext = new SQLContext(sc)

  def getSparkContext(): SparkContext = {
        return sc
  }

  def getSqlContext(): SQLContext = {
    return sqlContext
  }
}
