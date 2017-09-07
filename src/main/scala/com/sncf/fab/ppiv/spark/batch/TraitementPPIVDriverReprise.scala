package com.sncf.fab.ppiv.spark.batch

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
object TraitementPPIVDriver extends Serializable {

  var LOGGER = LoggerFactory.getLogger(TraitementPPIVDriver.getClass)
  LOGGER.info("Lancement du batch PPIV REPRISE")

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
      val sc         = GetSparkEnv.getSparkContext()
      val sqlContext = GetSparkEnv.getSqlContext()
      val hiveContext = GetHiveEnv.getHiveContext(sc)

      // Set du niveau de log pour ne pas être envahi par les messages
      sc.setLogLevel("ERROR")


      // Sauvegarde de l'heure de début du programme dans une variable
      val startTimePipeline = Conversion.nowToDateTime()



      if (Conversion.validateDateInputFormatForADay(args(1)) == true  && args(2)== null)
        {

          LOGGER.info("Lancement du batch de reprise sur la journée de " + args(1).toString )
          println("Lancement du batch de reprise sur la journée " + args(1).toString )

          //le jour de la reprise sous le format dateTime
          val startTimeToProcess = Conversion.getDateTimeFromArgument(args(2)+"_00")
          val endTimeToProcess   = Conversion.getDateTimeFromArgument(args(3)+"_23")

          // Lancement du pipeline pour la journée demandé
          startPipelineReprise(args, sc, sqlContext, hiveContext,startTimeToProcess,endTimeToProcess, true)

        }

     if(Conversion.validateDateInputFormat(args(1)) == true && Conversion.validateDateInputFormat(args(2)) == true){
        //  - 3 arguments (persistance, date début, date fin) et dates valides -> Lancement du batch sur la période spécifié
        LOGGER.info("Lancement du batch de reprise sur la période spécifié entre " + args(1).toString + " et " + args(2).toString)
        println("Lancement du batch de reprise sur la période spécifié entre " + args(1).toString + " et " + args(2).toString)


        // Enregistrement de la début et de la fin de la période dans le format dateTime a partir du format string yyyyMMdd_HH
        val startTimeToProcess = Conversion.getDateTimeFromArgument(args(1))
        val endTimeToProcess   = Conversion.getDateTimeFromArgument(args(2))


        // Création d'une période pour pouvoir manipuler plus facilement l'interval
        val period = new Duration(startTimeToProcess, endTimeToProcess)

        // Décompte du nombre d'heures sur la période
        val nbHours = period.toStandardHours.getHours()


          println("Lancement du Pipeline pour la période: " + Conversion.getHourDebutPlageHoraire(startTimeToProcess) + " et " + Conversion.getHourFinPlageHoraire(endTimeToProcess))
          LOGGER.info("Lancement du Pipeline pour la date/Time: " + startTimeToProcess.toString())

          // Lancement du pipeline pour l'heure demandé
          startPipelineReprise(args, sc, sqlContext, hiveContext,startTimeToProcess,endTimeToProcess, false)

      }
      else{
        //  - 3 arguments (persistance, date début, date fin) mais dates invalide (les dates doivent être de la forme yyyyMMdd_HH) -> Stop
        LOGGER.error("Les dates de plage horaire ne sont pas dans le bon format yyyyMMdd_HH pour " + args(1) + " ou " + args(2))
        System.exit(1)
      }
    }
  }

  // Fonction appelé pour le déclenchement d'un pipeline complet pour une heure donnée
  def startPipelineReprise(argsArray: Array[String], sc: SparkContext, sqlContext: SQLContext, hiveContext: HiveContext, startTimeToProcess : DateTime,endTimeToProcess :DateTime, reprise : Boolean): Unit = {

    // Récupération argument d'entrées, la méthode de persistance
    val persistMethod = argsArray(0)

    val ivTga = TraitementTga.start(sc, sqlContext, hiveContext, startTimeToProcess ,endTimeToProcess, reprise)
    val ivTgd = TraitementTgd.start(sc, sqlContext, hiveContext, startTimeToProcess ,endTimeToProcess, reprise)

    // 11) Fusion des résultats de TGA et TGD
    LOGGER.info("11) Fusion des résultats entre TGA et TGD")
    val ivTgaTgd = ivTga.unionAll(ivTgd)


    try {
      // 12) Persistence dans la méthode demandée (hdfs, hive, es, fs)
      LOGGER.warn("Persistence dans la méthode demandée (hdfs, hive, es, fs)")

      Persist.save(ivTgaTgd, persistMethod, sc, startTimeToProcess, hiveContext)

      LOGGER.warn("SUCCESS")
      // Voir pour logger le succès
      PpivRejectionHandler.write_execution_message("OK", startTimePipeline.toString(),"","")
    }
    catch {
      case e: Throwable => {
        e.printStackTrace()
        PpivRejectionHandler.handleRejection("KO",startTimePipeline.toString(),"","enregistrement dans Hive. Exception: " + e.getMessage)
      }
    }
  }

}

