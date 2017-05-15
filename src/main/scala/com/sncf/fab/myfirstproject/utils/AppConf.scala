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
  val cdTGA = "TGA.csv"
  val TGD = "TGD.csv"

}

