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
    val dataRefGares              = LoadData.loadReferentiel(sqlContext)

    // 2) Application du sparadrap sur les données au cause du Bug lié au passe nuit
    // On le conditionne a un flag dans app.conf car dans le futur Obier compte patcher le bug

    if (STICKING_PLASTER == true) LOGGER.info("Flag sparadrap activé, application de la correction")
    val dataTgaTgdBugFix = if (STICKING_PLASTER == true) Preprocess.applyStickingPlaster(dataTgaTgd, sqlContext) else dataTgaTgd

    // 3) Validation champ à champ
    LOGGER.info("Validation champ à champ")
    val (dataTgaTgdFielValidated, dataTgaTgdFielRejected)   = ValidateData.validateField(dataTgaTgdBugFix, sqlContext)


    // 4) Reconstitution des évènements pour chaque trajet
    // L'objectif de cette fonction est de renvoyer (cycleId | Array(TgaTgdInput) pour les cyclesId terminé
    // Et les TgaTgdInput de tout le cycle de vie du train (toute la journée + journée précédente pour les passe nuits)
    LOGGER.info("Reconstitution des cycles avec les évènements terminés")
    val cycleWithEventOver = BuildCycleOver.getCycleOver(dataTgaTgdFielValidated, sc, sqlContext, Panneau())


    // 5) Boucle sur les cycles finis pour traiter leur liste d'évènements
    LOGGER.info("Traitement des cycles terminés")

    val rddIvTgaTgdWithoutReferentiel = cycleWithEventOver.map{ x =>

      // Récupération du cycleId (première colonne)
      var cycleId = x.getString(0)

      // Récupération de la séquence de String (deuxième colonne)
      val seqString = x.getSeq[String](1)

      // Transsformation des séquences de string en Seq[TgaTgdInput)
      val seqTgaTgd = seqString.map(x => {
        // Les champs sont séparés par des virgules
        val split = x.toString.split(",",-1)
        TgaTgdInput(split(0), split(1).toLong, split(2), split(3), split(4), split(5), split(6), split(7), split(8), split(9).toLong, split(10), split(11))
      })


      // 6) Validation des cycles
      val isCycleValidated  = ValidateData.validateCycle(seqTgaTgd)
      if(isCycleValidated == false){
        // TODO : Regarder si il n'a pas un état en IND ou SUP
        // Si oui on enregistre la ligne avec les infos qu'on a
        LOGGER.info("Cycle invalide pour le cycle Id: " + cycleId)
        cycleId = "INV_" + cycleId
        //TgaTgdIntermediate("INV_" + cycleId,seqTgaTgd(0).gare,seqTgaTgd(0).ordes,seqTgaTgd(0).num,seqTgaTgd(0).`type`,seqTgaTgd(0).heure,seqTgaTgd(0).etat, 0, 0, 0,0,0,0,0,0,"","","","","",0,0)
      }

      // 7) Nettoyage et mise en forme
      // On se sait pas si on en aura besoin, on la laisse en attendant
      val dataTgaTgdCycleCleaned    = Preprocess.cleanCycle(seqTgaTgd)

      // 8) Calcul des différents règles de gestion.
      val premierAffichage = BusinessRules.getPremierAffichage(dataTgaTgdCycleCleaned)
      val affichageDuree1  = BusinessRules.getAffichageDuree1(dataTgaTgdCycleCleaned)
      val affichageDuree2  = BusinessRules.getAffichageDuree2(dataTgaTgdCycleCleaned)
      val dernier_retard_annonce = BusinessRules.getDernierRetardAnnonce(dataTgaTgdCycleCleaned)
      val affichage_retard = BusinessRules.getAffichageRetard(dataTgaTgdCycleCleaned)
      val affichage_duree_retard = BusinessRules.getAffichageDureeRetard(dataTgaTgdCycleCleaned)
      val etat_train  = BusinessRules.getEtatTrain(dataTgaTgdCycleCleaned)
      val date_affichage_etat_train = BusinessRules.getDateAffichageEtatTrain(dataTgaTgdCycleCleaned)
      val delai_affichage_etat_train_avant_depart_arrive = BusinessRules.getDelaiAffichageEtatTrainAvantDepartArrive(dataTgaTgdCycleCleaned)
      val dernier_quai_affiche = BusinessRules.getDernierQuaiAffiche(dataTgaTgdCycleCleaned)
      val type_devoiement = BusinessRules.getTypeDevoiement(dataTgaTgdCycleCleaned)
      val type_devoiement2 = BusinessRules.getTypeDevoiement2(dataTgaTgdCycleCleaned)
      val type_devoiement3 = BusinessRules.getTypeDevoiement3(dataTgaTgdCycleCleaned)
      val type_devoiement4 = BusinessRules.getTypeDevoiement4(dataTgaTgdCycleCleaned)
      val dernier_affichage = BusinessRules.getDernierAffichage(dataTgaTgdCycleCleaned)
      val date_process = BusinessRules.getDateProcess(dataTgaTgdCycleCleaned)


      // 9) Création de la classe de sortie sans le référentiel
      TgaTgdIntermediate(cycleId,seqTgaTgd(0).gare,seqTgaTgd(0).ordes,seqTgaTgd(0).num,seqTgaTgd(0).`type`,seqTgaTgd(0).heure,etat_train,
        premierAffichage, affichageDuree1,dernier_retard_annonce, affichageDuree2,affichage_retard,affichage_duree_retard,
        date_affichage_etat_train,delai_affichage_etat_train_avant_depart_arrive, dernier_quai_affiche,type_devoiement,type_devoiement2,
          type_devoiement3,type_devoiement4, dernier_affichage, date_process )
    }



    // Conversion du résulat en dataset
    val dsIvTgaTgdWithoutReferentiel = rddIvTgaTgdWithoutReferentiel.toDS()


    // 10) Filtre sur les cyles qui ont été validé ou non
    val cycleInvalidated = dsIvTgaTgdWithoutReferentiel.toDF().filter($"cycleId".contains("INV")).as[TgaTgdIntermediate]

    //Persist.save(cycleInvalidated.toDF() , "hiveRejet", sc)

    val cycleValidated    = dsIvTgaTgdWithoutReferentiel.toDF().filter(not($"cycleId".contains("INV"))).as[TgaTgdIntermediate]

    println("invalidated:" + cycleInvalidated.count())




    // 11) Enregistrement des rejets (champs + cycle)
    Reject.saveFieldRejected(dataTgaTgdFielRejected, sc)
    Reject.saveCycleRejected(cycleInvalidated, sc)

    // 12) Sauvegarde des cycles d'évènements validés
    // A partir de cycleValidate :
    // Dataset[TgaTgdWithoutRef] -> DataSet[TgaTgdInput]
    // Puis enregistrer dans l'object PostProcess
    //PostProcess.saveCleanData(DataSet[TgaTgdInput], sc)

    // 13) Jointure avec le référentiel
    val dataTgaTgdWithReferentiel = Postprocess.joinReferentiel(cycleValidated, dataRefGares, sqlContext)

    println("ref:" + dataTgaTgdWithReferentiel.count())

    println("Dernier Quai AFfiche ------------------------------------------------------------------------------")
    dataTgaTgdWithReferentiel.select("dernier_quai_affiche").show()
    // 14) Inscription dans la classe finale TgaTgdOutput avec conversion et formatage
    val dataTgaTgdOutput = Postprocess.formatTgaTgdOuput(dataTgaTgdWithReferentiel, sqlContext, Panneau())

    // On renvoie le data set final pour un Tga ou un Tgd (qui seront fusionné dans le main)
    dataTgaTgdOutput
  }
}



