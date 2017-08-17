package com.sncf.fab.ppiv.pipelineData.libPipeline

import com.fasterxml.jackson.module.scala.util.Options
import com.sncf.fab.ppiv.business.{TgaTgdInput, TgaTgdIntermediate}
import com.sncf.fab.ppiv.spark.batch.TraitementPPIVDriver.LOGGER
import com.sncf.fab.ppiv.utils.Conversion
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.joda.time.DateTime

import scala.collection.immutable.{ListMap, SortedMap}

/**
  * Created by ELFI03951 on 12/07/2017.
  */
object BusinessRules {

  // Traitement des cycles valides + calcul des KPIs
  def computeBusinessRules(
      cycleWithEventOver: DataFrame, timeToProcess: DateTime): RDD[TgaTgdIntermediate] = {

    // On boucle sur tous les cycles ID
    val rddIvTgaTgdWithoutReferentiel = cycleWithEventOver.map { x =>

      // Récupération du cycleId (première colonne)
      var cycleId = x.getString(0)

      // Récupération de la séquence de String (deuxième colonne)
      //val seqString = x.getSeq[String](1)
      val seqString = x.getString(1).split(",").toSeq

      // Transsformation des séquences de string en Seq[TgaTgdInput)
      val seqTgaTgd = seqString.map(x => {
        // Les champs sont séparés par des virgules
        val split = x.toString.split(";", -1)
        TgaTgdInput(split(0),
                    split(1).toLong,
                    split(2),
                    split(3),
                    split(4),
                    split(5),
                    split(6),
                    split(7),
                    split(8),
                    split(9).toLong,
                    split(10),
                    split(11))
      })

      // 6) Validation des cycles


      val isCycleValidated = ValidateData.validateCycle(seqTgaTgd)
      if (isCycleValidated._1 == false) {
        // Raison de l'invalidation
        val rejectReason = isCycleValidated._2
        // TODO : Regarder si il n'a pas un état en IND ou SUP
        // Si oui on enregistre la ligne avec les infos qu'on a
        LOGGER.info("Cycle invalide pour le cycle Id: " + cycleId)
        cycleId = "INV_" + cycleId
        TgaTgdIntermediate(
          cycleId,
          seqTgaTgd(0).gare,
          seqTgaTgd(0).ordes,
          seqTgaTgd(0).num,
          seqTgaTgd(0).`type`,
          seqTgaTgd(0).heure,
          seqTgaTgd(0).etat,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          rejectReason,
          "",
          "",
          "",
          "",
          0,
          0
        )
      } else {

        // 7) Nettoyage et mise en forme
        // On se sait pas si on en aura besoin, on la laisse en attendant
        val dataTgaTgdCycleCleaned = Preprocess.cleanCycle(seqTgaTgd)

        // 8) Calcul des différents règles de gestion.
        val premierAffichage =
          BusinessRules.getPremierAffichage(dataTgaTgdCycleCleaned)
        val affichageDuree1 =
          BusinessRules.getAffichageDuree1(dataTgaTgdCycleCleaned)
        val affichageDuree2 =
          BusinessRules.getAffichageDuree2(dataTgaTgdCycleCleaned)
        val dernier_retard_annonce =
          BusinessRules.getDernierRetardAnnonce(dataTgaTgdCycleCleaned)
        val affichage_retard =
          BusinessRules.getAffichageRetard(dataTgaTgdCycleCleaned)
        val affichage_duree_retard =
          BusinessRules.getAffichageDureeRetard(dataTgaTgdCycleCleaned)
        val etat_train = BusinessRules.getEtatTrain(dataTgaTgdCycleCleaned)
        val date_affichage_etat_train =
          BusinessRules.getDateAffichageEtatTrain(dataTgaTgdCycleCleaned)
        val delai_affichage_etat_train_avant_depart_arrive =
          BusinessRules.getDelaiAffichageEtatTrainAvantDepartArrive(
            dataTgaTgdCycleCleaned)
        val dernier_quai_affiche =
          BusinessRules.getDernierQuaiAffiche(dataTgaTgdCycleCleaned)
        val type_devoiement =
          BusinessRules.getTypeDevoiement(dataTgaTgdCycleCleaned)
        val type_devoiement2 =
          BusinessRules.getTypeDevoiement2(dataTgaTgdCycleCleaned)
        val type_devoiement3 =
          BusinessRules.getTypeDevoiement3(dataTgaTgdCycleCleaned)
        val type_devoiement4 =
          BusinessRules.getTypeDevoiement4(dataTgaTgdCycleCleaned)
        val dernier_affichage =
          BusinessRules.getDernierAffichage(dataTgaTgdCycleCleaned)
        val date_process = BusinessRules.getDateProcess(timeToProcess)

        // 9) Création de la classe de sortie sans le référentiel
        TgaTgdIntermediate(
          cycleId,
          seqTgaTgd(0).gare,
          seqTgaTgd(0).ordes,
          seqTgaTgd(0).num,
          seqTgaTgd(0).`type`,
          seqTgaTgd(0).heure,
          etat_train,
          premierAffichage,
          affichageDuree1,
          dernier_retard_annonce,
          affichageDuree2,
          affichage_retard,
          affichage_duree_retard,
          date_affichage_etat_train,
          delai_affichage_etat_train_avant_depart_arrive,
          dernier_quai_affiche,
          type_devoiement,
          type_devoiement2,
          type_devoiement3,
          type_devoiement4,
          dernier_affichage,
          date_process
        )
      }
    }
    rddIvTgaTgdWithoutReferentiel
  }

  // Retourne 0 si pas de retard ou le retard dans le cas échéant en secondes
  def getCycleRetard(dsTgaTgd: Seq[TgaTgdInput]): Long = {

    // Tri sur les horaires d'évènements en croissant puis filtre sur la colonne retard
    val seqFiltered = dsTgaTgd
      .sortBy(x => x.maj)
      .filter(x => (x.retard != null) && (x.retard != "") && (x.retard != "0"))

    // Si pas de lignes retournée => pas de retard on revoie 0
    if (seqFiltered.isEmpty) {
      0
    } else {

      // Récupération du dernier retard. -1 pour aller chercher sur le dernier index
      val minuteRetard = seqFiltered(seqFiltered.length - 1).retard.toLong

      // Multipliation par 60 pour renvoyer un résultat en secondes
      minuteRetard * 60
    }
  }

  // Fonction qui renvoie la date de premier affichage de la voie en timestamp pour un cycle donné
  def getPremierAffichage(seqTgaTgd: Seq[TgaTgdInput]): Long = {

    // Récupération de la date de premier affichage. On cherche le moment ou la bonne voie a été affiché pour la première fois
    try {
      seqTgaTgd
        .sortBy(_.maj)
        .reverse
        .filter(x => x.voie != null && x.voie != "" && x.voie != ("0"))
        .groupBy(_.voie)
        .toSeq
        .map(row => (row._1, row._2.maxBy(_.maj), row._2.minBy(_.maj)))
        .sortBy(_._2.maj)
        .last
        ._3
        .maj
    }
    catch {
      case e: Throwable => {
        // Retour d'une valeur par défaut
        0l
      }
    }
  }

  // Fonction qui renvoie le timestamp durant lequel le train est resté affiché
  def getAffichageDuree1(seqTgaTgdSeq: Seq[TgaTgdInput]): Long = {

    try{
      //Récupération de la date du départ theorique
      val departTheorique = seqTgaTgdSeq(0).heure.toLong

      // Récupération de la date du premier affichage
      val premierAffichage = getPremierAffichage(seqTgaTgdSeq)

      departTheorique - premierAffichage

    }
    catch {
      case e: Throwable => {
        // Retour d'une valeur par défaut
        0l
      }
    }
  }

  // Fonction qui renvoie le timestamp durant lequel le train est resté affiché retard inclus
  def getAffichageDuree2(seqTgaTgd: Seq[TgaTgdInput]): Long = {

    try{
      //Récupération du affichageDuree1
      val affichageDuree1 = getAffichageDuree1(seqTgaTgd)

      //Récupération du retard
      val retard = getCycleRetard(seqTgaTgd)

      affichageDuree1 + retard
    }
    catch {
      case e: Throwable => {
        // Retour d'une valeur par défaut
        0l
      }
    }
  }

  //Fonction qui renvoie le dernier retard annonce en secondes
  def getDernierRetardAnnonce(seqTgaTgd: Seq[TgaTgdInput]): Long = {

    val DernierRetardAnnonce = getCycleRetard(seqTgaTgd)

    DernierRetardAnnonce
  }

  //Fonction qui renvoie la date de l'affichage du retard por la première fois
  def getAffichageRetard(seqTgaTgd: Seq[TgaTgdInput]): Long = {


    try{
      // Tri sur les horaires d'évènements en croissant puis filtre sur la colonne retard
      val seqFiltered = seqTgaTgd
        .sortBy(x => x.maj)
        .filter(x => (x.retard != null) && (x.retard != "") && (x.retard != "0"))
        .sortBy(x => x.maj)

      // Si pas de lignes retournée => pas de retard on revoie 0
      if (seqFiltered.isEmpty) {
        0
      } else {
        // Récupération du permier  retard.
        val AffichegeRetard = seqFiltered(0).maj.toLong
        AffichegeRetard
      }
    }
    catch {
      case e: Throwable => {
        // Retour d'une valeur par défaut
        0
      }
    }
  }
  // Fonction qui renvoie la durée de l'affichage du retrad (si pas de retard  elle renvoie 0)
  def getAffichageDureeRetard(seqTgaTgd: Seq[TgaTgdInput]): Long = {


    try{
      val departTheorique = seqTgaTgd(0).heure.toLong
      val retard = getCycleRetard(seqTgaTgd)
      val departReel = (Conversion
        .unixTimestampToDateTime(departTheorique)
        .plusSeconds(retard.toInt))
        .getMillis / 1000
      val AffichageRetard = getAffichageRetard(seqTgaTgd)

      if (AffichageRetard != 0) departReel - AffichageRetard
      else 0l

    }
    catch {
      case e: Throwable => {
        // Retour d'une valeur par défaut
        0l
      }
    }

  }

  //Fonction qui renvoie l'état du train
  def getEtatTrain(seqTgaTgd: Seq[TgaTgdInput]): String = {
    // si au moins une fois IND alors Ind: Retard Inderminé
    // si une fois SUPP alors Supp: Train supprimé


    try{
      val seqFiltered = seqTgaTgd
        .sortBy(x => x.maj)
        .filter(x => (x.etat != null) && (x.etat != ""))
      if (seqFiltered.isEmpty) {
        null
      } else {
        if (seqFiltered.contains("IND")) { "IND" } else {
          if (seqFiltered.contains("SUPP")) { "SUP" } else {
            seqFiltered(0).etat.toString
          }
        }
      }

    }
    catch {
      case e: Throwable => {
        // Retour d'une valeur par défaut
        ""
      }
    }
  }

  //Fonction qui renvoie la date de l'affichage de l'etat deu train
  //TODO: adapt it to getEtatTrain
  def getDateAffichageEtatTrain(seqTgaTgd: Seq[TgaTgdInput]): Long = {

    try{
      val seqFiltered = seqTgaTgd
        .sortBy(x => x.maj)
        .filter(x => (x.etat != null) && (x.etat != ""))
      if (seqFiltered.isEmpty) {
        0
      } else {
        seqFiltered(0).maj.toLong
      }
    }
    catch {
      case e: Throwable => {
        // Retour d'une valeur par défaut
        0l
      }
    }
  }

  //Fonction qui renvoie la durée entre l'affiche de l'etat du train  et le départ théorique
  def getDelaiAffichageEtatTrainAvantDepartArrive(
      seqTgaTgd: Seq[TgaTgdInput]): Long = {

    try{
      //Récupération de la date du départ théorique
      val departTheorique = seqTgaTgd(0).heure.toLong
      //Récupération de la date Affichage etat train
      val DateAffichageEtatTrain = getDateAffichageEtatTrain(seqTgaTgd)
      if (DateAffichageEtatTrain != 0) {
        departTheorique - DateAffichageEtatTrain
      } else {
        0
      }
    }
    catch {
      case e: Throwable => {
        // Retour d'une valeur par défaut
        0l
      }
    }
  }

  //Fonction qui renvoie le dernier quai affiché
  def getDernierQuaiAffiche(seqTgaTgd: Seq[TgaTgdInput]): String = {


    try{
      //filtrage des lignes avec voies
      val dsVoie = seqTgaTgd
        .sortBy(_.maj)
        .reverse
        .filter(x => x.voie != null && x.voie != "" && x.voie != ("0"))
      if (dsVoie.isEmpty) {
        null
      } else {
        dsVoie(0).voie.toString
      }

    }
    catch {
      case e: Throwable => {
        // Retour d'une valeur par défaut
        ""
      }
    }
  }

  //Fonction qui renvoie InfoDevoiement
  def allDevoimentInfo(
      seqTgaTgd: Seq[TgaTgdInput]): Seq[(String, TgaTgdInput, Int)] = {

    try{

      val dsVoie = seqTgaTgd
        .sortBy(_.maj)
        .filter(x => x.voie != null && x.voie != "" && x.voie != ("0"))
        .groupBy(_.voie)

      // Astuce pour ordonner le group by. Fréquellent utilisé a voir pour créer une fonction
      val dvInfo = dsVoie.toSeq
        .map(
          row =>
            (row._1,
              row._2.maxBy(_.maj),
              row._2.filter(_.attribut_voie != "I").length))
        .sortBy(_._2.maj)

      dvInfo

    }
    catch {
      case e: Throwable => {
        // Retour d'une valeur par défaut
        Seq(("",null,0))
      }
    }
  }

  //Fonction qui renvoie le type du devoiement1 (Affiche ou non Affiché)
  def getTypeDevoiement(seqTgaTgd: Seq[TgaTgdInput]): String = {

    try{

      val dvInfo = allDevoimentInfo(seqTgaTgd)
      if (dvInfo.size <= 1) {
        null
      } else {
        val voie_1 = dvInfo(0)._2.voie
        val voie_2 = dvInfo(1)._2.voie
        val lengthOfNonHidden_Voie = dvInfo(1)._3

        if (lengthOfNonHidden_Voie >= 1) {
          voie_1 + "-" + voie_2 + "-" + "Affiche"
        } else {
          voie_1 + "-" + voie_2 + "-" + "Non_Affiche"
        }
      }
    }
    catch {
      case e: Throwable => {
        // Retour d'une valeur par défaut
        ""
      }
    }
  }

  //Fonction qui renvoie le type du devoiement2 (Affiche ou non Affiché)
  def getTypeDevoiement2(seqTgaTgd: Seq[TgaTgdInput]): String = {

    try{
      val dvInfo = allDevoimentInfo(seqTgaTgd)
      if (dvInfo.size <= 2) {
        null
      } else {
        val voie_1 = dvInfo(1)._2.voie
        val voie_2 = dvInfo(2)._2.voie
        val lengthOfNonHidden_Voie = dvInfo(2)._3

        if (lengthOfNonHidden_Voie >= 1) {
          voie_1 + "-" + voie_2 + "-" + "Affiche"
        } else {
          voie_1 + "-" + voie_2 + "-" + "Non_Affiche"
        }
      }
    }
    catch {
      case e: Throwable => {
        // Retour d'une valeur par défaut
        ""
      }
    }
  }

  //Fonction qui renvoie le type du devoiement2 (Affiche ou non Affiché)
  def getTypeDevoiement3(seqTgaTgd: Seq[TgaTgdInput]): String = {

    try{
      val dvInfo = allDevoimentInfo(seqTgaTgd)
      if (dvInfo.size <= 3) {
        null
      } else {
        val voie_1 = dvInfo(2)._2.voie
        val voie_2 = dvInfo(3)._2.voie
        val lengthOfNonHidden_Voie = dvInfo(3)._3

        if (lengthOfNonHidden_Voie >= 1) {
          voie_1 + "-" + voie_2 + "-" + "Affiche"
        } else {
          voie_1 + "-" + voie_2 + "-" + "Non_Affiche"
        }
      }

    }
    catch {
      case e: Throwable => {
        // Retour d'une valeur par défaut
        ""
      }
    }
  }

  //Fonction qui renvoie le type du devoiement4 (Affiche ou non Affiché)
  def getTypeDevoiement4(seqTgaTgd: Seq[TgaTgdInput]): String = {

    try{
      val dvInfo = allDevoimentInfo(seqTgaTgd)
      if (dvInfo.size <= 4) {
        null
      } else {
        val voie_1 = dvInfo(3)._2.voie
        val voie_2 = dvInfo(4)._2.voie
        val lengthOfNonHidden_Voie = dvInfo(4)._3

        if (lengthOfNonHidden_Voie >= 1) {
          voie_1 + "-" + voie_2 + "-" + "Affiche"
        } else {
          voie_1 + "-" + voie_2 + "-" + "Non_Affiche"
        }
      }
    }
    catch {
      case e: Throwable => {
        // Retour d'une valeur par défaut
        ""
      }
    }
  }

  //Fonction qui retourne la date du dernier affichage
  def getDernierAffichage(seqTgaTgd: Seq[TgaTgdInput]): Long = {

    try{
      val dernier_affichage = seqTgaTgd
        .sortBy(_.maj)
        .filter(x => x.voie != null && x.voie != "" && x.voie != ("0"))
        .groupBy(_.voie)
        .toSeq
        .map(row => (row._1, row._2.maxBy(_.maj)))
        .sortBy(_._2.maj).reverse.head._2.maj

      dernier_affichage
    }
    catch {
      case e: Throwable => {
        // Retour d'une valeur par défaut
        0l
      }
    }


  }

  //Fonction qui renvoie la date a laquelle le batch s'est lancé pour un trajet
  def getDateProcess(timeToProcess: DateTime): Long = {
    try{
      timeToProcess.getMillis / 1000
    }
    catch {
      case e: Throwable => {
        // Retour d'une valeur par défaut
        0l
      }
    }

  }

}
