package com.sncf.fab.ppiv.spark.batch

import java.time.Period

import com.sncf.fab.ppiv.Exception.PpivRejectionHandler
import com.sncf.fab.ppiv.persistence._
import com.sncf.fab.ppiv.pipelineData.{SourcePipeline, TraitementTga, TraitementTgd}
import org.apache.log4j.Logger
import com.sncf.fab.ppiv.utils.AppConf._
import com.sncf.fab.ppiv.utils.{Conversion, GetHiveEnv, GetSparkEnv}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.{DateTime, Duration}
import org.slf4j.LoggerFactory
import com.sncf.fab.ppiv.pipelineData.libPipeline._
import org.apache.spark.sql.hive.HiveContext
import org.apache.log4j.{Level, LogManager, PropertyConfigurator}

/**
//  * Created by simoh-labdoui on 11/05/2017.
//  */

// Classe main, lancement du programme
object TraitementPPIVDriver extends Serializable {


  // Définition du logger Spark
  val LOGGER = LogManager.getRootLogger
  LOGGER.setLevel(Level.WARN)


  // Sauvegarde de l'heure de début du programme dans une variable
  val startTimePipeline = Conversion.nowToDateTime()
  val endTimePipeline   = Conversion.nowToDateTime()
  val reprise = false


  def main(args: Array[String]): Unit = {
    // 5 cas de figure pour l'exécution du programme
    //  - Pas d'arguments d'entrée -> Stop
    //  - Argument n°1 de persistance non valide -> Stop
    //  - 1 seul et unique argument valide (hive, hdfs, es, fs) -> Nominal : Lancement automatique du batch sur l'heure n-1
    //  - 2 arguments (persistance, date/heure à processer) mais dates invalide (les dates doivent être de la forme yyyyMMdd_HH) -> Stop
    //  - 2 arguments (persistance, date/heure à processer) et dates valides -> Lancement du batch sur la période spécifié



    if (args.length == 0){
      // Pas d'arguments d'entrée -> Stop
      PpivRejectionHandler.handleRejection("KO","",startTimePipeline.toString(),"","Pas d'arguments d'entrée, le batch nécessite au minimum la méthode de persistance (hdfs, hive, fs, es)")
    }
    else if(!(args(0).contains("hdfs") || args(0).contains("fs") || args(0).contains("es") || args(0).contains("hive")) ){
      // Argument n°1 de persistance non valide -> Stop
      PpivRejectionHandler.handleRejection("KO","",startTimePipeline.toString(),"","Pas de méthode de persistence (hdfs, fs, hive ou es pour l'agument" + args(0).toString)
    }
    else {

      // Définition du Spark Context et SQL Context à partir de utils/GetSparkEnv
      try{

        val sc         = GetSparkEnv.getSparkContext()
        val sqlContext = GetSparkEnv.getSqlContext()
        val hiveContext = GetHiveEnv.getHiveContext(sc)



        if(args.length == 1){
          //  - 1 seul et unique argument valide (hive, hdfs, es, fs) -> Nominal : Lancement automatique du batch sur l'heure n-1
          LOGGER.warn("Lancement automatique du batch sur l'heure n-1")
          startPipeline(args, sc, sqlContext, hiveContext, startTimePipeline, endTimePipeline, reprise)
        }
        else if(Conversion.validateDateInputFormat(args(1)) == true){

          // On a une heure a processer en paramètre

          //  - 3 arguments (persistance, date début, date fin) et dates valides -> Lancement du batch sur la période spécifié
          LOGGER.warn("Lancement du batch pour l'heure : " + args(1).toString)

          val startTimeToProcess = Conversion.getDateTimeFromArgument(args(1))
          val endTimeToProcess = Conversion.getDateTimeFromArgument(args(1))

          println(startTimeToProcess.toString())

          // Lancement du pipeline pour l'heure demandé (+ 1 car le pipelin est construit par rapport a ce qu'on lui donne l'heure de fin de traitement
          startPipeline(args, sc, sqlContext, hiveContext, startTimeToProcess.plusHours(1), endTimeToProcess, reprise)


        }
        else{
          //  - 3 arguments (persistance, date début, date fin) mais dates invalide (les dates doivent être de la forme yyyyMMdd_HH) -> Stop
          PpivRejectionHandler.handleRejection("KO",Conversion.getHourDebutPlageHoraire(startTimePipeline),startTimePipeline.toString(),"","Les dates de plage horaire ne sont pas dans le bon format yyyyMMdd_HH pour " + args(1) + " ou " + args(2))
        }
      }
      catch {
        case e: Throwable => {
          // Retour d'une valeur par défaut
          e.printStackTrace()
          PpivRejectionHandler.handleRejection("KO",Conversion.getHourDebutPlageHoraire(startTimePipeline), startTimePipeline.toString(),"","Pb driver principal. exception: " + e)
        }
      }
    }
  }

  // Fonction appelé pour le déclenchement d'un pipeline complet pour une heure donnée
  def startPipeline(argsArray: Array[String], sc: SparkContext, sqlContext: SQLContext, hiveContext: HiveContext, startTimeToProcess: DateTime, endTimeToProcess: DateTime, reprise: Boolean): Unit = {

    import sqlContext.implicits._

    // Récupération argument d'entrées, la méthode de persistance
    val persistMethod = argsArray(0)


    LOGGER.warn("Processing des TGA")
    val ivTga = TraitementTga.start(sc, sqlContext, hiveContext, startTimeToProcess,endTimeToProcess, reprise)

    LOGGER.warn("Processing des TGD")
    val ivTgd = TraitementTgd.start(sc, sqlContext, hiveContext,startTimeToProcess,endTimeToProcess,reprise)



    // 11) Fusion des résultats de TGA et TGD
    LOGGER.warn("TGA et TGD traités enregistrement")
    val ivTgaTgd = ivTga.unionAll(ivTgd)

    try {
      // 12) Persistence dans la méthode demandée (hdfs, hive, es, fs)
      LOGGER.warn("Persistence dans la méthode demandée (hdfs, hive, es, fs)")

      Persist.save(ivTgaTgd, persistMethod, sc, startTimeToProcess, hiveContext)

      LOGGER.warn("SUCCESS")
      // Voir pour logger le succès
      PpivRejectionHandler.write_execution_message("OK", Conversion.getHourDebutPlageHoraire(startTimeToProcess), startTimePipeline.toString(),"","")
    }
    catch {
      case e: Throwable => {
        e.printStackTrace()
        PpivRejectionHandler.handleRejection("KO", Conversion.getHourDebutPlageHoraire(startTimeToProcess), startTimePipeline.toString(),"","enregistrement dans Hive. Exception: " + e.getMessage)
      }
    }
  }

}

