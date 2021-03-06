package com.sncf.fab.ppiv.test.utils

import com.typesafe.config.ConfigFactory


/**
  * Created by simoh-labdoui on 10/05/2017.
  */
object AppConf extends Serializable{
  val conf         = ConfigFactory.load()

  val SPARK_MASTER = conf.getString("spark-master")
  val PPIV         = conf.getString("ppiv")

  // Chemin ou aller chercher et enregistrer les données
  val GOLD         = conf.getString("gold")
  val REFINERY     = conf.getString("refinery")
  val LANDING_WORK = conf.getString("landing_work")
  val REF_GARES    = conf.getString("ref_gares")

  // Chemin ou enregistrer les logs
  val LOG_PATH     = conf.getString("log")

  // Variable pour le prijet
  val STICKING_PLASTER = conf.getBoolean("apply_sticking_plaster")

  // Valeur TGA et TGD
  val TGA          = "TGA.csv"
  val TGD          = "TGD.csv"

  // elastic confs
  val PORT= conf.getString("port")
  val HOST= conf.getString("host")
  val OUTPUT_INDEX         =conf.getString("ivTgaTgdIndex")
  val FIELD_REJECTED_INDEX =conf.getString("rejectFieldValidationIndex")
  val CYCLE_REJECTED_INDEX =conf.getString("rejectCycleValidationIndex")






}

