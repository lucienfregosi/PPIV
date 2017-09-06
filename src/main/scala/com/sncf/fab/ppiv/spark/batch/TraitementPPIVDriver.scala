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

  def main(args: Array[String]): Unit = {
    // 5 cas de figure pour l'exécution du programme
    //  - Pas d'arguments d'entrée -> Stop
    //  - Argument n°1 de persistance non valide -> Stop
    //  - 1 seul et unique argument valide (hive, hdfs, es, fs) -> Nominal : Lancement automatique du batch sur l'heure n-1
    //  - 3 arguments (persistance, date début, date fin) mais dates invalide (les dates doivent être de la forme yyyyMMdd_HH) -> Stop
    //  - 3 arguments (persistance, date début, date fin) et dates valides -> Lancement du batch sur la période spécifié

    if (args.length == 0){
      // Pas d'arguments d'entrée -> Stop
      LOGGER.error("Pas d'arguments d'entrée, le batch nécessite au minimum la méthode de persistance (hdfs, hive, fs, es)")
      System.exit(1)
    }
    else if(!(args(0).contains("hdfs") || args(0).contains("fs") || args(0).contains("es") || args(0).contains("hive")) ){
      // Argument n°1 de persistance non valide -> Stop
      LOGGER.error("Pas de méthode de persistence (hdfs, fs, hive ou es pour l'agument" + args(0).toString)
      System.exit(1)
    }
    else {

      // Définition du Spark Context et SQL Context à partir de utils/GetSparkEnv
      try{
        val sc         = GetSparkEnv.getSparkContext()
        val sqlContext = GetSparkEnv.getSqlContext()
        val hiveContext = GetHiveEnv.getHiveContext(sc)

        // Sauvegarde de l'heure de début du programme dans une variable
        val startTimePipeline = Conversion.nowToDateTime()


        if(args.length == 1){
          //  - 1 seul et unique argument valide (hive, hdfs, es, fs) -> Nominal : Lancement automatique du batch sur l'heure n-1
          LOGGER.warn("Lancement automatique du batch sur l'heure n-1")
          startPipeline(args, sc, sqlContext, hiveContext, startTimePipeline)
        }
        else if(Conversion.validateDateInputFormat(args(1)) == true && Conversion.validateDateInputFormat(args(2)) == true){
          //  - 3 arguments (persistance, date début, date fin) et dates valides -> Lancement du batch sur la période spécifié
          LOGGER.warn("Lancement du batch sur la période spécifié entre " + args(1).toString + " et " + args(2).toString)

          // Enregistrement de la début et de la fin de la période dans le format dateTime a partir du format string yyyyMMdd_HH
          val startTimeToProcess = Conversion.getDateTimeFromArgument(args(1))
          val endTimeToProcess   = Conversion.getDateTimeFromArgument(args(2))

          // Création d'une période pour pouvoir manipuler plus facilement l'interval
          val period = new Duration(startTimeToProcess, endTimeToProcess)

          // Décompte du nombre d'heures sur la période
          val nbHours = period.toStandardHours.getHours()

          // Boucle sur la totalité des heures
          for(hoursIterator <- 0 to nbHours){
            // Calcul de la dateTime a passer en paramètre au pipeline
            val newDateTime = startTimeToProcess.plusHours(hoursIterator)

            // Lancement du pipeline pour l'heure demandé
            startPipeline(args, sc, sqlContext, hiveContext, newDateTime)
          }
        }
        else{
          //  - 3 arguments (persistance, date début, date fin) mais dates invalide (les dates doivent être de la forme yyyyMMdd_HH) -> Stop
          PpivRejectionHandler.handleRejection("Les dates de plage horaire ne sont pas dans le bon format yyyyMMdd_HH pour " + args(1) + " ou " + args(2))
        }
      }
      catch {
        case e: Throwable => {
          // Retour d'une valeur par défaut
          e.printStackTrace()
          PpivRejectionHandler.handleRejection("KO driver principal. exception: " + e)
        }
      }
    }
  }

  // Fonction appelé pour le déclenchement d'un pipeline complet pour une heure donnée
  def startPipeline(argsArray: Array[String], sc: SparkContext, sqlContext: SQLContext, hiveContext: HiveContext, dateTimeToProcess: DateTime): Unit = {

    import sqlContext.implicits._

    // Récupération argument d'entrées, la méthode de persistance
    val persistMethod = argsArray(0)

    LOGGER.warn("Processing des TGA")
    val ivTga = TraitementTga.start(sc, sqlContext, hiveContext, dateTimeToProcess)

    LOGGER.warn("Processing des TGD")
    val ivTgd = TraitementTgd.start(sc, sqlContext, hiveContext, dateTimeToProcess)


    // 11) Fusion des résultats de TGA et TGD
    LOGGER.warn("TGA et TGD traités enregistrement")
    val ivTgaTgd = ivTga.unionAll(ivTgd)

    try {
      // 12) Persistence dans la méthode demandée (hdfs, hive, es, fs)
      LOGGER.warn("Persistence dans la méthode demandée (hdfs, hive, es, fs)")

      Persist.save(ivTgaTgd, persistMethod, sc, dateTimeToProcess, hiveContext)

      LOGGER.warn("SUCCESS")
      // Voir pour logger le succès
    }
    catch {
      case e: Throwable => {
        e.printStackTrace()
        PpivRejectionHandler.handleRejection("KO enregistrement dans Hive. Exception: " + e.getMessage)
      }
    }
  }

}

