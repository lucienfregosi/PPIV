package com.sncf.fab.ppiv.spark.batch

import java.time.Period

import com.sncf.fab.ppiv.Exception.PpivRejectionHandler
import com.sncf.fab.ppiv.persistence._
import com.sncf.fab.ppiv.pipelineData.{SourcePipeline, TraitementTga, TraitementTgd}
import org.apache.log4j.Logger
import com.sncf.fab.ppiv.utils.AppConf._
import com.sncf.fab.ppiv.utils.{Conversion, GetSparkEnv}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.{DateTime, Duration}

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

      // Sauvegarde de l'heure de début du programme dans une variable
      val startTimePipeline = Conversion.nowToDateTime()

      // Lancement du pipeline en fonction du mode choisi
      if(args.length == 1){
        // Fonctionnement nominal du programme on utilise l'heure actuelle
        LOGGER.info("Fonctionnement nominal du pipeline pour l'heure n-1")
        startPipeline(args, sc, sqlContext, startTimePipeline)
      }
      else if(Conversion.validateDateInputFormat(args(1)) == true && Conversion.validateDateInputFormat(args(2)) == true){
        LOGGER.info("Fonctionnement entre deux plages horaires")
        println("2 plages horaires")
        // TODO: Boucler sur toutes les dates qui nous intéressent

        val startTimeToProcess = Conversion.getDateTimeFromArgument(args(1))
        val endTimeToProcess   = Conversion.getDateTimeFromArgument(args(2))

        val period = new Duration(startTimeToProcess, endTimeToProcess)

        // Renvoie le nombre d'heures
        val nbHours = period.toStandardHours.getHours()

        var hoursIterator = 0
        for(hoursIterator <- 0 to nbHours){
          // Calcul de la dateTime pour lequel il faut processer
          val newDateTime = startTimeToProcess.plusHours(hoursIterator)
          LOGGER.info("Lancement du Pipeline pour la date: " + newDateTime.toString())
          //startPipeline(args, sc, sqlContext, newDateTime)
        }
        System.exit(0)
      }
      else{
        LOGGER.error("Les dates de plage horaire ne sont pas dans le bon format yyyyMMdd_HH pour " + args(1) + " ou " + args(2))
        System.exit(1)
      }
    }
  }

  def startPipeline(argsArray: Array[String], sc: SparkContext, sqlContext: SQLContext, dateTimeToProcess: DateTime): Unit = {
    // Définition argument d'entrée
    val persistMethod = argsArray(0)

    LOGGER.info("Traitement d'affichage des TGA")
    val ivTga = TraitementTga.start(sc, sqlContext, dateTimeToProcess)


    LOGGER.info("Traitement d'affichage des TGD")
    //val ivTgd = TraitementTgd.start(sc, sqlContext)

    // 11) Fusion des résultats de TGA et TGD
    //val ivTgaTgd = ivTga.union(ivTgd)

    // 12) Persistence dans la brique demandé

    try {
      Persist.save(ivTga, persistMethod, sc, dateTimeToProcess)
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

