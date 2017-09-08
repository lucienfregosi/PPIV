package com.sncf.fab.ppiv.pipelineData


import java.nio.file.{Files, Paths}

import com.sncf.fab.ppiv.Exception.PpivRejectionHandler
import com.sncf.fab.ppiv.business._
import com.sncf.fab.ppiv.parser.DatasetsParser
import com.sncf.fab.ppiv.persistence.Persist
import com.sncf.fab.ppiv.pipelineData.libPipeline.LoadData.checkIfFileExist
import com.sncf.fab.ppiv.pipelineData.libPipeline._
import com.sncf.fab.ppiv.spark.batch.TraitementPPIVDriver.startTimePipeline
import com.sncf.fab.ppiv.spark.batch.TraitementPPIVDriver.LOGGER
import com.sncf.fab.ppiv.utils.AppConf._
import com.sncf.fab.ppiv.utils.Conversion
import org.apache.spark.sql._
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel
import org.joda.time.{DateTime, DateTimeZone}

/**
  * Created by simoh-labdoui on 11/05/2017.
  */
trait SourcePipeline extends Serializable {

  /**
    *
    * @return le nom de l'application spark visible dans historyserver
    */
  def getAppName(): String = {
    PPIV
  }

  /**
    * @return le chemin de la source de données brute
    */
  def getSource(timeToProcess: DateTime, reprise: Boolean): String
  /**
    *
    * @return le chemin de l'output qualité
    */
  def getOutputGoldPath(timeToProcess: DateTime): String

  /**
    *
    * @return the path used to store the cleaned TgaTgaPased
    */

  def getOutputRefineryPath(timeToProcess: DateTime): String

  def getRejectCycleRefineryPath(timeToProcess: DateTime): String
  def getRejectCycleGoldPath(timeToProcess: DateTime): String
  def getRejectFieldRefineryPath(timeToProcess: DateTime): String
  def getRejectFieldGoldPath(timeToProcess: DateTime): String


  /**
    *
    * @return vrai s'il s'agit d'un départ de train, faux s'il s'agit d'un arrivé
    */
  def Depart(): Boolean

  /**
    *
    * @return faux s'il s'agit d'un départ de train, vrai s'il s'agit d'un arrivé
    */
  def Arrive(): Boolean

  /**
    *
    * @return TGA ou TGD selon le type de trajet
    */
  def Panneau(): String

  // Lancement du pipeline de traitement soit les TGA ou les TGD
  def start(sc : SparkContext, sqlContext : SQLContext, hiveContext: HiveContext,  startTimeToProcess : DateTime, endTimeToProcesse: DateTime, reprise: Boolean): DataFrame = {

    import sqlContext.implicits._


    try {
      // 1) Chargement des fichiers déjà parsé dans leur classe
      // Test si le fichier existe
      val pathFileToLoad = getSource( startTimeToProcess , reprise)


      val dataTgaTgd = LoadData.loadTgaTgd(sqlContext, pathFileToLoad, reprise)
      val dataRefGares = LoadData.loadReferentiel(sqlContext)

      println(getOutputRefineryPath(startTimeToProcess))

      System.exit(0)


      LOGGER.warn("Chargement des fichiers OK")

      try {
        // 2) Application du sparadrap sur les données au cause du Bug lié au patsse nuit (documenté dans le wiki)
        // On le conditionne a un flag (apply_sticking_plaster) dans app.conf car dans le futur Obier compte patcher le bug
        LOGGER.info("2) [OPTIONNEL] Application du sparadrap sur les données au cause du Bug lié au passe nuit")
        val dataTgaTgdBugFix = if (STICKING_PLASTER == true) {
          val returnValue = Preprocess.applyStickingPlaster(dataTgaTgd, sqlContext)
          LOGGER.warn("Application du sparadrap OK")
          returnValue
        } else dataTgaTgd

        try {
          // 3) Validation champ à champ
          val (dataTgaTgdFielValidated, dataTgaTgdFielRejected) = ValidateData.validateField(dataTgaTgdBugFix, sqlContext)
          LOGGER.warn("Validation champ à champ OK")

          try {
            // 4) Reconstitution des évènements pour chaque trajet
            // L'objectif de cette fonction est de renvoyer (cycleId | Array(TgaTgdInput) ) afin d'associer à chaque cycle de vie
            // d'un train terminé la liste de tous ses évènements en vue du calcul des indicateurs

            val cycleWithEventOver = BuildCycleOver.getCycleOver(dataTgaTgdFielValidated, sc, sqlContext, Panneau(),  startTimeToProcess , endTimeToProcesse, reprise)
            LOGGER.warn("Filtre des cycles Terminés OK")



            try {
              // 5) Boucle sur les cycles finis pour traiter leur liste d'évènements
              val rddIvTgaTgdWithoutReferentiel = BusinessRules.computeBusinessRules(cycleWithEventOver,  startTimeToProcess )
              LOGGER.warn("Calcul des indicateurs OK")


              try {
                // Conversion du résulat en dataset
                val dsIvTgaTgdWithoutReferentiel = rddIvTgaTgdWithoutReferentiel.toDS()

                // Filtre sur les cycles invalidés
                val cycleInvalidated = dsIvTgaTgdWithoutReferentiel.toDF().filter($"cycleId".contains("INV_")).as[TgaTgdIntermediate]
                val cycleValidated = dsIvTgaTgdWithoutReferentiel.toDF().filter(not($"cycleId".contains("INV_"))).as[TgaTgdIntermediate]


                Reject.saveFieldRejected(dataTgaTgdFielRejected, getRejectFieldRefineryPath(startTimeToProcess))
                Reject.saveCycleRejected(cycleInvalidated, getRejectCycleRefineryPath(startTimeToProcess))

                LOGGER.warn("Enregistrement des rejets OK")


                try {
                  // 10) Jointure avec le référentiel et inscription dans la classe finale TgaTgdOutput avec conversion et formatage
                  val dataTgaTgdOutput = Postprocess.postprocess(cycleValidated, dataRefGares, sqlContext, Panneau())
                  LOGGER.warn("Post Processing OK")


                  // On renvoie le data set final pour un Tga ou un Tgd (qui seront fusionné dans le main)

                  dataTgaTgdOutput


                }
                catch {
                  case e: Throwable => {
                    // Retour d'une valeur par défaut
                    e.printStackTrace()
                    PpivRejectionHandler.handleRejection("KO",Conversion.getHourFinPlageHoraire(startTimeToProcess),startTimePipeline.toString(),getSource(startTimeToProcess, reprise), "PostTraitement et jointure avec le referentiel: " + e)

                    null
                  }
                }
              }
              catch {
                case e: Throwable => {
                  // Retour d'une valeur par défaut
                  e.printStackTrace()
                  PpivRejectionHandler.handleRejection("KO",Conversion.getHourFinPlageHoraire(startTimeToProcess),startTimePipeline.toString(),getSource(startTimeToProcess, reprise), "Enregisrement des rejets: " + e)


                  null
                }
              }
            }

            catch {
              case e: Throwable => {
                // Retour d'une valeur par défaut
                e.printStackTrace()

                PpivRejectionHandler.handleRejection("KO",Conversion.getHourFinPlageHoraire(startTimeToProcess),startTimePipeline.toString(),getSource(startTimeToProcess, reprise), "Calcul des indicateurs: " + e)

                null
              }
            }
          }
          catch {
            case e: Throwable => {
              // Retour d'une valeur par défaut
              e.printStackTrace()

              PpivRejectionHandler.handleRejection("KO",Conversion.getHourFinPlageHoraire(startTimeToProcess),startTimePipeline.toString(),getSource(startTimeToProcess, reprise), "Constitution des cycles terminés: " + e)

              null
            }
          }
        }
        catch {
          case e: Throwable => {
            // Retour d'une valeur par défaut
            e.printStackTrace()

            PpivRejectionHandler.handleRejection("KO",Conversion.getHourFinPlageHoraire(startTimeToProcess),startTimePipeline.toString(),getSource(startTimeToProcess, reprise), "Validation Champ à champ: " + e)

            null
          }
        }
      }
      catch {
        case e: Throwable => {
          // Retour d'une valeur par défaut
          e.printStackTrace()

          PpivRejectionHandler.handleRejection("KO",Conversion.getHourFinPlageHoraire(startTimeToProcess),startTimePipeline.toString(),getSource(startTimeToProcess, reprise), "Application du sparadrap: " + e)

          null
        }
      }
    }
    catch {
      case e: Throwable => {
        // Retour d'une valeur par défaut
        e.printStackTrace()

        PpivRejectionHandler.handleRejection("KO",Conversion.getHourFinPlageHoraire(startTimeToProcess),startTimePipeline.toString(),getSource(startTimeToProcess, reprise), "KO Chargement des fichiers: " + e)

        null
      }
    }
  }
}



