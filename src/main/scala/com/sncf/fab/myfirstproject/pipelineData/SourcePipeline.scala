package com.sncf.fab.myfirstproject.pipelineData

import com.sncf.fab.myfirstproject.utils.AppConf
import com.sncf.fab.myfirstproject.utils.AppConf._
import org.apache.spark.sql.SparkSession

/**
  * Created by simoh-labdoui on 11/05/2017.
  */
trait SourcePipeline extends Serializable{



  def getAppName(): String = {
  AppConf.PPIV
  }
  def getSource():String

  def start() : Unit = {

    val sparkSession = SparkSession.builder.
      master(SPARK_MASTER)
      .appName(PPIV)
      .getOrCreate()

    process()
  }

  /**
    * scan file
    */
  def process():Unit= {
    /**
      * convertir les date, cleaner la data, filtrer la data, a sauvegarder dans un rep /landing/work
      */

    /**
      * Croiser la data avec le refernetiel et sauvegarder dans  un Gold
      */



  }


}



