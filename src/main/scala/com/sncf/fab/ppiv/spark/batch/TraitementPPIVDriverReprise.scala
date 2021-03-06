package com.sncf.fab.ppiv.spark.batch

import com.sncf.fab.ppiv.Exception.PpivRejectionHandler
import com.sncf.fab.ppiv.persistence._
import com.sncf.fab.ppiv.pipelineData.{TraitementTga, TraitementTgd}
import com.sncf.fab.ppiv.utils.{Conversion, GetSparkEnv}
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.util.SizeEstimator
import org.joda.time.DateTime

/**
//  * Created by simoh-labdoui on 11/05/2017.
//  */

// Classe main, lancement du programme
object TraitementPPIVDriverReprise extends Serializable {

  val LOGGER = LogManager.getRootLogger
  LOGGER.setLevel(Level.WARN)



  // Sauvegarde de l'heure de début du programme dans une variable
  val startTimePipeline = Conversion.nowToDateTime()

  def main(args: Array[String]): Unit = {
    // 5 cas de figure pour l'exécution du programme
    //  - Pas d'arguments d'entrée -> Stop
    //  - Argument n°1 de persistance non valide -> Stop
    //  - 1 seul et unique argument valide (hive, hdfs, es, fs) -> Nominal : Lancement automatique du batch sur l'heure n-1
    //  - 3 arguments (persistance, date début, date fin) mais dates invalide (les dates doivent être de la forme yyyyMMdd_HH) -> Stop
    //  - 3 arguments (persistance, date début, date fin) et dates valides -> Lancement du batch sur la période spécifié


    LOGGER.warn("Démarrage de l'application PPIV-Reprise")

    if (args.length == 0){
      // Pas d'arguments d'entrée -> Stop
      PpivRejectionHandler.handleRejectionError("KO","",startTimePipeline.toString(),"","Pas d'arguments d'entrée, le batch nécessite au minimum la méthode de persistance (hdfs, hive, fs, es)")
    }
    else if(!(args(0).contains("hdfs") || args(0).contains("fs") || args(0).contains("es") || args(0).contains("hive")) ){
      // Argument n°1 de persistance non valide -> Stop
      PpivRejectionHandler.handleRejectionError("KO","",startTimePipeline.toString(),"","Pas de méthode de persistence (hdfs, fs, hive ou es pour l'agument" + args(0).toString)
    }
    else {

      try{
        // Définition du Spark Context et SQL Context à partir de utils/GetSparkEnv
        val sc         = GetSparkEnv.getSparkContext()
        val sqlContext = GetSparkEnv.getSqlContext()


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
          startPipelineReprise(args, sc, sqlContext,debutPeriode, finPeriode)
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
          startPipelineReprise(args, sc, sqlContext,debutPeriode,finPeriode)

        }
        else{
          //  - 3 arguments (persistance, date début, date fin) mais dates invalide (les dates doivent être de la forme yyyyMMdd_HH) -> Stop
          PpivRejectionHandler.handleRejectionError("KO","",startTimePipeline.toString(),"","Les dates de plage horaire ne sont pas dans le bon format yyyyMMdd_HH pour " + args(1) + " ou " + args(2))
        }
      }
      catch {
        case e: Throwable => {
          // Retour d'une valeur par défaut
          PpivRejectionHandler.handleRejectionError("KO","",startTimePipeline.toString(),"","Pb driver principal. exception: " + e)
        }
      }
    }
  }

  // Fonction appelé pour le déclenchement d'un pipeline complet pour une heure donnée
  def startPipelineReprise(argsArray: Array[String], sc: SparkContext, sqlContext: SQLContext, debutPeriode : DateTime, finPeriode :DateTime): Unit = {

    // Récupération argument d'entrées, la méthode de persistance
    val persistMethod = argsArray(0)

    val ivTga = TraitementTga.start(sc, sqlContext, debutPeriode ,finPeriode, true)

    val ivTgd = TraitementTgd.start(sc, sqlContext, debutPeriode ,finPeriode, true)

    // 11) Fusion des résultats de TGA et TGD
    LOGGER.info("11) Fusion des résultats entre TGA et TGD")
    val ivTgaTgd = ivTga.unionAll(ivTgd)


    try {
      // 12) Persistence dans la méthode demandée (hdfs, hive, es, fs)
      LOGGER.warn("Persistence dans la méthode demandée (hdfs, hive, es, fs)")

      Persist.save(ivTgaTgd, persistMethod, sc, debutPeriode, true)


      // l'envoie du OK (statut = 0) à graphite pour dire que tout s'est bien passé
      PpivRejectionHandler.manageGraphite(0)

      // Ecriture d'une ligne dans le fichier de sortie execution
      PpivRejectionHandler.write_execution_message("OK",debutPeriode.toString(), startTimePipeline.toString(),"","")

      LOGGER.warn("OK")

      LOGGER.warn(" Taille du fichier de sortie en Byte : " +  SizeEstimator.estimate(ivTgaTgd.rdd))

      LOGGER.warn("temps d'execution en secondes: " + ((Conversion.nowToDateTime().getMillis - startTimePipeline.getMillis) / 1000 ))
    }
    catch {
      case e: Throwable => {
        PpivRejectionHandler.handleRejectionError("KO",debutPeriode.toString(), startTimePipeline.toString(),"","enregistrement dans Hive. Exception: " + e)
      }
    }
  }

}

