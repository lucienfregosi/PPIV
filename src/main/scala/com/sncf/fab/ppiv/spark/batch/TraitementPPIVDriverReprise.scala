package com.sncf.fab.ppiv.spark.batch

import java.io.{PrintWriter, StringWriter}

import com.sncf.fab.ppiv.Exception.PpivRejectionHandler
import com.sncf.fab.ppiv.persistence._
import com.sncf.fab.ppiv.pipelineData.{TraitementTga, TraitementTgd}
import com.sncf.fab.ppiv.spark.batch.TraitementPPIVDriver.startTimePipeline
import com.sncf.fab.ppiv.utils.{Conversion, GetHiveEnv, GetSparkEnv}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.joda.time.{DateTime, Duration}
import org.slf4j.LoggerFactory

/**
//  * Created by simoh-labdoui on 11/05/2017.
//  */

// Classe main, lancement du programme
object TraitementPPIVDriverReprise extends Serializable {

  var LOGGER = LoggerFactory.getLogger(TraitementPPIVDriver.getClass)
  LOGGER.info("Lancement du batch PPIV REPRISE")

  // Sauvegarde de l'heure de début du programme dans une variable
  val startTimePipeline = Conversion.nowToDateTime()

  def main(args: Array[String]): Unit = {
    // 5 cas de figure pour l'exécution du programme
    //  - Pas d'arguments d'entrée -> Stop
    //  - Argument n°1 de persistance non valide -> Stop
    //  - 1 seul et unique argument valide (hive, hdfs, es, fs) -> Nominal : Lancement automatique du batch sur l'heure n-1
    //  - 3 arguments (persistance, date début, date fin) mais dates invalide (les dates doivent être de la forme yyyyMMdd_HH) -> Stop
    //  - 3 arguments (persistance, date début, date fin) et dates valides -> Lancement du batch sur la période spécifié

    if (args.length == 0){
      // Pas d'arguments d'entrée -> Stop
      PpivRejectionHandler.handleRejection("KO","",startTimePipeline.toString(),"","Pas d'arguments d'entrée, le batch nécessite au minimum la méthode de persistance (hdfs, hive, fs, es)")
    }
    else if(!(args(0).contains("hdfs") || args(0).contains("fs") || args(0).contains("es") || args(0).contains("hive")) ){
      // Argument n°1 de persistance non valide -> Stop
      PpivRejectionHandler.handleRejection("KO","",startTimePipeline.toString(),"","Pas de méthode de persistence (hdfs, fs, hive ou es pour l'agument" + args(0).toString)
    }
    else {

      try{
        // Définition du Spark Context et SQL Context à partir de utils/GetSparkEnv
        val sc         = GetSparkEnv.getSparkContext()
        val sqlContext = GetSparkEnv.getSqlContext()
        val hiveContext = GetHiveEnv.getHiveContext(sc)

        // Set du niveau de log pour ne pas être envahi par les messages
        sc.setLogLevel("ERROR")

        // 2 cas de figure
        // Un argument YYYYMMDD => On fait la reprise sur une journée entière
        // Debut Periode : YYYYMMDD_00 Fin période : YYYYMM(DD+1)_00

        // 2 argument type YYYYMMDD_N1 à YYYYMMDD_N2
        // Debut Periode YYYYMMDD_N1 Fin periode YYYYMMDD_(N2+1)


        // Une journée a rattraper
        // 1er arugment : Persistance
        // 2ème une date en format YYYYMMDD
        if ( args.length == 2 && Conversion.validateDateInputFormatForADay(args(1)) == true ) {

          // 1er cas de figure
          LOGGER.warn("Lancement du batch de reprise sur la journée de " + args(1).toString )

          val debutPeriodeZone = Conversion.getDateTimeFromArgument(args(1) + "_00")
          val debutPeriode = Conversion.getDateTime(
            debutPeriodeZone.getYear,
            debutPeriodeZone.getMonthOfYear,
            debutPeriodeZone.getDayOfMonth,
            debutPeriodeZone.getHourOfDay,
            0,
            0)

          val finPeriodeZone = Conversion.getDateTimeFromArgument(args(1) + "_00")

          val finPeriode = Conversion.getDateTime(
            debutPeriodeZone.getYear,
            debutPeriodeZone.getMonthOfYear,
            debutPeriodeZone.getDayOfMonth,
            debutPeriodeZone.getHourOfDay,
            0,
            0).plusDays(1)



          // Lancement du pipeline pour la journée demandé
          startPipelineReprise(args, sc, sqlContext, hiveContext,debutPeriode, finPeriode)
        }
        else if(Conversion.validateDateInputFormat(args(1)) == true && Conversion.validateDateInputFormat(args(2)) == true){
          //  - 3 arguments (persistance, date début, date fin) et dates valides -> Lancement du batch sur la période spécifié
          LOGGER.warn("Lancement du batch de reprise sur la période spécifié entre " + args(1).toString + " et " + args(2).toString)

          // 2ème cas de figure

          val debutPeriodeZone = Conversion.getDateTimeFromArgument(args(1))
          val debutPeriode = Conversion.getDateTime(
            debutPeriodeZone.getYear,
            debutPeriodeZone.getMonthOfYear,
            debutPeriodeZone.getDayOfMonth,
            debutPeriodeZone.getHourOfDay,
            0,
            0)

          val finPeriodeZone = Conversion.getDateTimeFromArgument(args(2))
          val finPeriode = Conversion.getDateTime(
            debutPeriodeZone.getYear,
            debutPeriodeZone.getMonthOfYear,
            debutPeriodeZone.getDayOfMonth,
            debutPeriodeZone.getHourOfDay,
            0,
            0)


          // Lancement du pipeline pour l'heure demandé
          startPipelineReprise(args, sc, sqlContext, hiveContext,debutPeriode,finPeriode)

        }
        else{
          //  - 3 arguments (persistance, date début, date fin) mais dates invalide (les dates doivent être de la forme yyyyMMdd_HH) -> Stop
          PpivRejectionHandler.handleRejection("KO","",startTimePipeline.toString(),"","Les dates de plage horaire ne sont pas dans le bon format yyyyMMdd_HH pour " + args(1) + " ou " + args(2))
        }
      }
      catch {
        case e: Throwable => {
          // Retour d'une valeur par défaut
          val sw = new StringWriter
          e.printStackTrace(new PrintWriter(sw))
          PpivRejectionHandler.handleRejection("KO","",startTimePipeline.toString(),"","Pb driver principal. exception: " + e)
        }
      }
    }
  }

  // Fonction appelé pour le déclenchement d'un pipeline complet pour une heure donnée
  def startPipelineReprise(argsArray: Array[String], sc: SparkContext, sqlContext: SQLContext, hiveContext: HiveContext, debutPeriode : DateTime, finPeriode :DateTime): Unit = {

    // Récupération argument d'entrées, la méthode de persistance
    val persistMethod = argsArray(0)

    val ivTga = TraitementTga.start(sc, sqlContext, hiveContext, debutPeriode ,finPeriode, true)


    val ivTgd = TraitementTgd.start(sc, sqlContext, hiveContext, debutPeriode ,finPeriode, true)

    // 11) Fusion des résultats de TGA et TGD
    LOGGER.info("11) Fusion des résultats entre TGA et TGD")
    val ivTgaTgd = ivTga.unionAll(ivTgd)


    try {
      // 12) Persistence dans la méthode demandée (hdfs, hive, es, fs)
      LOGGER.warn("Persistence dans la méthode demandée (hdfs, hive, es, fs)")

      Persist.save(ivTgaTgd, persistMethod, sc, debutPeriode, hiveContext, true)

      LOGGER.warn("SUCCESS")
      // Voir pour logger le succès
      PpivRejectionHandler.write_execution_message("OK",debutPeriode.toString(), startTimePipeline.toString(),"","")
    }
    catch {
      case e: Throwable => {
        val sw = new StringWriter
        e.printStackTrace(new PrintWriter(sw))
        PpivRejectionHandler.handleRejection("KO",debutPeriode.toString(), startTimePipeline.toString(),"","enregistrement dans Hive. Exception: " + e.getMessage)
      }
    }
  }

}

