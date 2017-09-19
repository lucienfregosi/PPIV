package com.sncf.fab.ppiv.spark.batch

import java.io.{PrintWriter, StringWriter}
import java.time.Period

import com.sncf.fab.ppiv.Exception.PpivRejectionHandler
import com.sncf.fab.ppiv.persistence._
import com.sncf.fab.ppiv.pipelineData.{SourcePipeline, TraitementTga, TraitementTgd}
import org.apache.log4j.Logger
import com.sncf.fab.ppiv.utils.AppConf._
import com.sncf.fab.ppiv.utils.{Conversion, GetHiveEnv, GetSparkEnv}
import com.sncf.fab.ppiv.Monitoring.GraphiteConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.{DateTime, Duration}
import org.slf4j.LoggerFactory
import com.sncf.fab.ppiv.pipelineData.libPipeline._
import org.apache.spark.sql.hive.HiveContext
import org.apache.log4j.{Level, LogManager, PropertyConfigurator}
import scala.reflect.runtime.universe
import scala.tools.reflect.ToolBox

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


  def main(args: Array[String]): Unit = {
    // 5 cas de figure pour l'exécution du programme
    //  - Pas d'arguments d'entrée -> Stop
    //  - Argument n°1 de persistance non valide -> Stop
    //  - 1 seul et unique argument valide (hive, hdfs, es, fs) -> Nominal : Lancement automatique du batch sur l'heure n-1
    //  - 2 arguments (persistance, date/heure à processer) mais dates invalide (les dates doivent être de la forme yyyyMMdd_HH) -> Stop
    //  - 2 arguments (persistance, date/heure à processer) et dates valides -> Lancement du batch sur la période spécifié


    val date = Conversion.getDateTime(
      2017,
      9,
      3,
      23,
      0,
      0)


    val sc         = GetSparkEnv.getSparkContext()
    val sqlContext = GetSparkEnv.getSqlContext()

    import sqlContext.implicits._

    val dataTGA = BuildCycleOver.loadDataFullPeriod(sc,sqlContext,"TGA", date,date).toDF()
    val dataTGD = BuildCycleOver.loadDataFullPeriod(sc,sqlContext,"TGD", date,date).toDF()

    val data = dataTGA.unionAll(dataTGD)

    val hiveContext = GetHiveEnv.getHiveContext(sc)
    val dfHive = hiveContext.createDataFrame(data.rdd, data.schema)
    dfHive.registerTempTable("dataToSaveToHive")
    hiveContext.sql("CREATE TABLE iv_tgatgdinputrecette2 as select * from dataToSaveToHive")

    System.exit(0)


    LOGGER.warn("Démarrage de l'application PPIV")

    LOGGER.warn("Démarrage de l'application PPIV")

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
        //val hiveContext = GetHiveEnv.getHiveContext(sc)


        // 2 cas de figure, soit on a pas d'argument d'entrée
        // Dans ce cas on prend l'heure actuelle par ex 11h32
        // et on va chercher le fichier à n-1 donc celui de 10h qui contient tous les évènements de 10h a 11h
        // On lance donc source pipeline entre 10h et 11

        // 2ème cas de figure on précise l'heure a traiter : par exemple 20170908_11
        // Dans ce cas on traite le fichier de 11h qui contient les evènements de 11h a 12h

        if(args.length == 1){
          //  - 1 seul et unique argument valide (hive, hdfs, es, fs) -> Nominal : Lancement automatique du batch sur l'heure n-1
          LOGGER.warn("Lancement automatique du batch sur l'heure n-1")

          // Si startTimePipeline = 13h46

          // finPeriode = 13h00
          val finPeriode = Conversion.getDateTime(
            startTimePipeline.getYear,
            startTimePipeline.getMonthOfYear,
            startTimePipeline.getDayOfMonth,
            startTimePipeline.getHourOfDay,
            0,
            0)

          val debutPeriode = finPeriode.plusHours(-1)

          startPipeline(args, sc, sqlContext, debutPeriode, finPeriode)
        }
        else if(Conversion.validateDateInputFormat(args(1)) == true){

          // On a une heure a processer en paramètre

          //  - 3 arguments (persistance, date début, date fin) et dates valides -> Lancement du batch sur la période spécifié
          LOGGER.warn("Lancement du batch pour l'heure : " + args(1).toString)

          val debutPeriodeZone = Conversion.getDateTimeFromArgument(args(1))

          // On ne prend pas en compte les timezone, elles seront prises en compte plus tard
          val debutPeriode = Conversion.getDateTime(
            debutPeriodeZone.getYear,
            debutPeriodeZone.getMonthOfYear,
            debutPeriodeZone.getDayOfMonth,
            debutPeriodeZone.getHourOfDay,
            0,
            0)

          val finPeriode = debutPeriode.plusHours(1)



          // Lancement du pipeline pour l'heure demandé (+ 1 car le pipelin est construit par rapport a ce qu'on lui donne l'heure de fin de traitement
          startPipeline(args, sc, sqlContext, debutPeriode,finPeriode)

        }
        else{
          //  - 3 arguments (persistance, date début, date fin) mais dates invalide (les dates doivent être de la forme yyyyMMdd_HH) -> Stop
          PpivRejectionHandler.handleRejection("KO","",startTimePipeline.toString(),"","Les dates de plage horaire ne sont pas dans le bon format yyyyMMdd_HH pour " + args(1) + " ou " + args(2))
        }
      }
      catch {
        case e: Throwable => {
          // Catch final, c'est ici qu'on écrit dans le fichier de résultat

          PpivRejectionHandler.handleRejectionFinal("KO","",startTimePipeline.toString(),"","Exception relevé pendant l'execution: " + e)

        }
      }
    }
  }

  // Fonction appelé pour le déclenchement d'un pipeline complet pour une heure donnée
  def startPipeline(argsArray: Array[String], sc: SparkContext, sqlContext: SQLContext, debutPeriode: DateTime, finPeriode: DateTime): Unit = {

    import sqlContext.implicits._
    GraphiteConf.startGraphite()
    // Récupération argument d'entrées, la méthode de persistance
    val persistMethod = argsArray(0)

    LOGGER.warn("Processing des TGA")
    val ivTga = TraitementTga.start(sc, sqlContext, debutPeriode, finPeriode, false)

    LOGGER.warn("Processing des TGD")
    val ivTgd = TraitementTgd.start(sc, sqlContext, debutPeriode, finPeriode, false)


    // 11) Fusion des résultats de TGA et TGD
    LOGGER.warn("TGA et TGD traités enregistrement")
    val ivTgaTgd = ivTga.unionAll(ivTgd)

    try {
      // 12) Persistence dans la méthode demandée (hdfs, hive, es, fs)
      LOGGER.warn("Persistence dans la méthode demandée (hdfs, hive, es, fs)")

      // Sauvegarde dans HDFS
      Persist.save(ivTgaTgd, persistMethod, sc, debutPeriode, false)

      // Renommage du fichier car il a fini d'écrire
      Conversion.renameFile(TraitementTga.getOutputRefineryPathTMP(debutPeriode, finPeriode,false), TraitementTga.getOutputRefineryPath(debutPeriode, finPeriode,false))

      // Ecriture d'un fichier permettant aux scripts Hive de trouver les bon path
      Conversion.writeTmpFile(sc,sqlContext, TraitementTga.getOutputRefineryPath(debutPeriode, finPeriode,false), TraitementTga.getRejectCycleRefineryPath(debutPeriode, finPeriode,false), TraitementTga.getRejectFieldRefineryPath(debutPeriode, finPeriode,false) )


      // Voir pour logger le succès
      PpivRejectionHandler.write_execution_message("OK",debutPeriode.toString(), startTimePipeline.toString(),"","")

      LOGGER.warn("OK")

      LOGGER.warn("temps d'execution en secondes: " + ((Conversion.nowToDateTime().getMillis - startTimePipeline.getMillis) / 1000 ))


    }
    catch {
      case e: Throwable => {
        val sw = new StringWriter
        e.printStackTrace(new PrintWriter(sw))
        PpivRejectionHandler.handleRejection("KO",debutPeriode.toString(), startTimePipeline.toString(),"","Echec Enregistrement dans "+ persistMethod + ". Exception: " + e)
      }
    }
  }

}

