package com.sncf.fab.ppiv.spark.batch

import com.sncf.fab.ppiv.Exception.PpivRejectionHandler
import com.sncf.fab.ppiv.persistence._
import com.sncf.fab.ppiv.pipelineData.{SourcePipeline, TraitementTga, TraitementTgd}
import org.apache.log4j.Logger
import com.sncf.fab.ppiv.utils.AppConf._
import com.sncf.fab.ppiv.utils.GetSparkEnv
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
//  * Created by simoh-labdoui on 11/05/2017.
//  */

// Classe main, lancement du programme
object TraitementPPIVDriver extends Serializable {
  var LOGGER = Logger.getLogger(TraitementPPIVDriver.getClass)
  LOGGER.info("Lancement du batch PPIV")
  def main(args: Array[String]): Unit = {
    // Test des arguments d'entrée, on attend la brique sur laquelle persister (hdfs, hive, es ...)
    if (args.length == 0){
      LOGGER.error("Pas de paramètres d'entrée")
      System.exit(1)
    }
    else if(!(args(0).contains("hdfs") || args(0).contains("fs") || args(0).contains("es") || args(0).contains("hive")) ){
      LOGGER.error("Pas de méthode de persistence (hdfs, fs, hive ou es pour l'agument" + args(0).toString)
      System.exit(1)
    }
    else {

      // Définition du Spark Context et SQL Context à partir de utils/GetSparkEnv
      val sc         = GetSparkEnv.getSparkContext()
      val sqlContext = GetSparkEnv.getSqlContext()

      // Set du niveau de log pour ne pas être envahi par les messages
      sc.setLogLevel("ERROR")

      // Définition argument d'entrée
      val persistMethod = args(0)

      LOGGER.info("Traitement d'affichage des TGA")
      val ivTga = TraitementTga.start(sc, sqlContext)


      LOGGER.info("Traitement d'affichage des TGD")
      //val ivTgd = TraitementTgd.start(sc, sqlContext)

      // 11) Fusion des résultats de TGA et TGD
      //val ivTgaTgd = ivTga.union(ivTgd)

      // 12) Persistence dans la brique demandé

      try {
        Persist.save(ivTga, persistMethod, sc)
      }
      catch {
        case e: Throwable => {
          e.printStackTrace()
          PpivRejectionHandler.handleRejection(e.getMessage, PpivRejectionHandler.PROCESSING_ERROR)
          None
        }
      }


    }
  }
}

