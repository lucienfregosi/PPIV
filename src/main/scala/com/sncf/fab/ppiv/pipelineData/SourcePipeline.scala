package com.sncf.fab.ppiv.pipelineData


import com.sncf.fab.ppiv.business._
import com.sncf.fab.ppiv.parser.DatasetsParser
import com.sncf.fab.ppiv.persistence.Persist
import com.sncf.fab.ppiv.pipelineData.libPipeline._
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
  def getSource(): String
  /**
    *
    * @return le chemin de l'output qualité
    */
  def getOutputGoldPath(): String

  /**
    *
    * @return the path used to store the cleaned TgaTgaPased
    */

  def getOutputRefineryPath(): String


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

  // Lancement du pipeline de traitement pour TGA et TGD
  def start(sc : SparkContext, sqlContext : SQLContext): DataFrame = {

    import sqlContext.implicits._

    // 1) Chargement des fichiers déjà parsé dans leur classe
    LOGGER.info("Chargement des fichiers et du référentiel")
    val dataTgaTgd                = LoadData.loadTgaTgd(sqlContext, getSource())
   // dataTgaTgd.persist(StorageLevel.DISK_ONLY)
    //dataTgaTgd.unpersist()
    val dataRefGares              = LoadData.loadReferentiel(sqlContext)

    // 2) Application du sparadrap sur les données au cause du Bug lié au passe nuit
    // On le conditionne a un flag dans app.conf car dans le futur Obier compte patcher le bug

    if (STICKING_PLASTER == true) LOGGER.info("Flag sparadrap activé, application de la correction")
    val dataTgaTgdBugFix = if (STICKING_PLASTER == true) Preprocess.applyStickingPlaster(dataTgaTgd, sqlContext) else dataTgaTgd

    // 3) Validation champ à champ
    LOGGER.info("Validation champ à champ")
    val (dataTgaTgdFielValidated, dataTgaTgdFielRejected)   = ValidateData.validateField(dataTgaTgdBugFix, sqlContext)

    LOGGER.info("Validation champ à champ counts")


    // 4) Reconstitution des évènements pour chaque trajet
    // L'objectif de cette fonction est de renvoyer (cycleId | Array(TgaTgdInput) pour les cyclesId terminé
    // Et les TgaTgdInput de tout le cycle de vie du train (toute la journée + journée précédente pour les passe nuits)
    LOGGER.info("Reconstitution des cycles avec les évènements terminés")
    val cycleWithEventOver = BuildCycleOver.getCycleOver(dataTgaTgdFielValidated, sc, sqlContext, Panneau())

    // Temporary Save Finished cycles in HDFS
     //Persist.save(cycleWithEventOver.toDF() , "CyclFinistoH", sc)
    cycleWithEventOver



    // 5) Boucle sur les cycles finis pour traiter leur liste d'évènements
    LOGGER.info("Traitement des cycles terminés")
    val rddIvTgaTgdWithoutReferentiel = BusinessRules.computeBusinessRules(cycleWithEventOver)

    // Conversion du résulat en dataset
    val dsIvTgaTgdWithoutReferentiel = rddIvTgaTgdWithoutReferentiel.toDS()

    // 10) Filtre sur les cyles qui ont été validé ou non
    val cycleInvalidated = dsIvTgaTgdWithoutReferentiel.toDF().filter($"cycleId".contains("INV_")).as[TgaTgdIntermediate]
    val cycleValidated    = dsIvTgaTgdWithoutReferentiel.toDF().filter(not($"cycleId".contains("INV_"))).as[TgaTgdIntermediate]


    // 11) Enregistrement des rejets (champs + cycle)
    //Reject.saveFieldRejected(dataTgaTgdFielRejected, sc)
    //Reject.saveCycleRejected(cycleInvalidated, sc)

    val cycleInvalidatedDf = cycleInvalidated.toDF()
    val cycleValidatedDf   =  cycleValidated.toDF()

    println("invalidated:" + cycleInvalidatedDf.count())
    println("validated:" + cycleValidatedDf.count())

   Persist.save(cycleValidatedDf  , "InputPostprocss", sc)

    // 12) Sauvegarde des cycles d'évènements validés
    // A partir de cycleValidate :
    // Dataset[TgaTgdWithoutRef] -> DataSet[TgaTgdInput]
    // Puis enregistrer dans l'object PostProcess
    //PostProcess.saveCleanData(DataSet[TgaTgdInput], sc)

    // 13) Jointure avec le référentiel et nscription dans la classe finale TgaTgdOutput avec conversion et formatage
    val dataTgaTgdOutput = Postprocess.postprocess (cycleValidated, dataRefGares, sqlContext, Panneau())

    // On renvoie le data set final pour un Tga ou un Tgd (qui seront fusionné dans le main)
    dataTgaTgdOutput


  }
}



