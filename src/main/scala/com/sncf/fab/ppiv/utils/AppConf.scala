package com.sncf.fab.ppiv.utils
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

  // Set de la property System
  System.setProperty("log_path", LOG_PATH)

  // Variable pour le prijet
  val STICKING_PLASTER = conf.getBoolean("apply_sticking_plaster")
  val MARGE_APRES_DEPART_REEL = conf.getLong("marge_apres_depart_reel")

  // Valeur TGA et TGD
  val TGA          = "TGA.csv"
  val TGD          = "TGD.csv"

  // Noms des fichiers pour les rejets
  val REJECTED_FIELD = conf.getString("refinery") + conf.getString("reject_field")
  val REJECTED_CYCLE = conf.getString("refinery") + conf.getString("reject_cycle")

  // elastic confs
  val PORT= conf.getString("port")
  val HOST= conf.getString("host")
  val OUTPUT_INDEX         =conf.getString("ivTgaTgdIndex")
  val FIELD_REJECTED_INDEX =conf.getString("rejectFieldValidationIndex")
  val CYCLE_REJECTED_INDEX =conf.getString("rejectCycleValidationIndex")






}

