package com.sncf.fab.myfirstproject.utils
import com.typesafe.config.ConfigFactory


/**
  * Created by simoh-labdoui on 10/05/2017.
  */
object AppConf extends Serializable{
  val conf = ConfigFactory.load()

  val SPARK_MASTER = conf.getString("local")
  val SAMPLE = conf.getString("sample")
  val PPIV = conf.getString("ppiv")
  val GOLD = conf.getString("gold")
  val REFINERY = conf.getString("refinery")
  val LANDING_WORK = conf.getString("landing_work")
  val TGA = "TGA.csv"
  val TGD = "TGD.csv"
  val REF_GARES="referentiel.csv"

  //elastic confs
  val PORT= conf.getString("port")
  val HOST= conf.getString("host")
  val QUALITE_INDEX=conf.getString("qualite_index")
  val TGA_TGD_INDEX=conf.getString("tga_tgd_index")

  //hdfs paths
  val GOLD_HDFS = conf.getString("gold_hdfs")
  val REFINERY_HDFS = conf.getString("refinery_hdfs")
  val LANDING_WORK_HDFS = conf.getString("landing_work_hdfs")




}

