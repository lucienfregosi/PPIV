package com.sncf.fab.ppiv.pipelineData.libPipeline

import com.fasterxml.jackson.module.scala.util.Options
import com.sncf.fab.ppiv.business.{TgaTgdInput, TgaTgdIntermediate}
import com.sncf.fab.ppiv.spark.batch.TraitementPPIVDriver.LOGGER
import com.sncf.fab.ppiv.utils.AppConf.MARGE_APRES_DEPART_REEL
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.joda.time.DateTime
import scala.collection.mutable.ListBuffer

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
        manageInvalidateCycle(cycleId, seqTgaTgd, rejectReason)

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

        // Get des informations liés au dévoiement commun aux fonction de type dévoiement
        val devoiementInfo = BusinessRules.allDevoimentInfo(dataTgaTgdCycleCleaned)

        val type_devoiement =
          BusinessRules.getTypeDevoiement(dataTgaTgdCycleCleaned, devoiementInfo, 1)
        val type_devoiement2 =
          BusinessRules.getTypeDevoiement(dataTgaTgdCycleCleaned, devoiementInfo, 2)
        val type_devoiement3 =
          BusinessRules.getTypeDevoiement(dataTgaTgdCycleCleaned, devoiementInfo, 3)
        val type_devoiement4 =
          BusinessRules.getTypeDevoiement(dataTgaTgdCycleCleaned, devoiementInfo, 4)
        val dernier_affichage =
          BusinessRules.getDernierAffichage(dataTgaTgdCycleCleaned)
        val date_process = BusinessRules.getDateProcess(timeToProcess)

        // Gestion des rejets ou on a pu trouver de valeur, pas d'intéret a enregistrer dans la sortie
        // En particulier pour get premier affichage
        if (premierAffichage == 0) {
          val rejectReason = "pasPremierAffichage"
          manageInvalidateCycle(cycleId, seqTgaTgd, rejectReason)
        }
        else {
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
    // Prendre en compte le cas ou on a
    // A A "" "" A A
    // On veut renvoyer le premier affichage de la deuxième séquence de A et non le premier de la première
    // On doit gérer le cas ou la voie est toujours affichée
    try {

      val retard = getCycleRetard(seqTgaTgd)

      // Filtrer les lignes qui se passe après le départ du train (retard compris)
      // pour ne pas fausser le calcul
      // En comptant la marge
      // 10 minutes : pour la marge d'erreur imposé par le métier (défini dans app.conf)

      val seqWithoutEventAfterDeparture = seqTgaTgd.filter(x => x.maj < x.heure + retard + MARGE_APRES_DEPART_REEL)

      // Case Class créé pour l'occasion pour pouvoir garder notre nomenclature
      case class SeqShort(maj: Long, voie: String)

      // On remplace les voies vides par des ~
      val seqShort = seqWithoutEventAfterDeparture.map(x => SeqShort(
        x.maj,
        if(x.voie == "") x.voie.replace("","~") else x.voie
      ))

      // Tri de la séquence
      // On forme un tableau de maj par voie
      val groupByVoieDistincte = seqShort.sortBy(_.maj)
        .reverse
        .groupBy(_.voie)
        .map(x => (x._1, x._2.maxBy(_.maj).maj, x._2.minBy(_.maj).maj, x._2.map(x => x.maj)))
        .toSeq
        .sortBy(_._2)
        .reverse

      // Si on a qu'une voie distincte (IE : la voie est affiché sur toutes les lignes et toujours la même
      // On traite différemlent car le cas est beaucoup plus simple on renvoie la valeur mini de maj

      if(groupByVoieDistincte.length == 1){

        // On retourne la valeur minimum pour le groupement
        groupByVoieDistincte.take(1).last._3

      }
      else{

        // On extrait la séquence n°1 et n°2
        val sequence1 = groupByVoieDistincte.take(1).last
        val sequence2 = groupByVoieDistincte.take(2).last

        // On prend le max de seq2
        val maxSeq2 = sequence2._4.toList.max

        // Dans la séquence de la voie finale on garde les maj au dessus de maxSeq2
        val sequence1Filtered = sequence1._4.toList.filter(_ >= maxSeq2)

        // On renvoie la valeur minimum
        sequence1Filtered.min

      }
    }
    catch {
      case e: Throwable => {
        // Retour d'une valeur par défaut
        0
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
        val affichageRetard = seqFiltered(0).maj.toLong
        affichageRetard
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
        val departReel = departTheorique + retard.toInt

      val affichageRetard = getAffichageRetard(seqTgaTgd)

      if (affichageRetard != 0) departReel - affichageRetard
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
  // On ne prend pas en compte l'état ARR
  def getEtatTrain(seqTgaTgd: Seq[TgaTgdInput]): String = {
    // si au moins une fois IND alors Ind: Retard Inderminé
    // si une fois SUPP alors Supp: Train supprimé



      val seqFiltered = seqTgaTgd
        .sortBy(x => x.maj)
        .filter(x => (x.etat != null) && (x.etat != "") && (x.etat != "ARR"))

    val seqOfEtat = seqFiltered.map(row => row.etat)

    if (seqOfEtat.contains("IND")) {
      "IND"
    }
    else {
      if (seqOfEtat.contains("SUP")) {
        "SUP"
      }
      else ""
    }

  }

  //Fonction qui renvoie la date de l'affichage de l'etat deu train
  def getDateAffichageEtatTrain(seqTgaTgd: Seq[TgaTgdInput]): Long = {

    try{
      val seqFiltered = seqTgaTgd
        .sortBy(x => x.maj)
        .filter(x => (x.etat != null) && (x.etat != "") && (x.etat != "ARR"))
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
        .filter(x => x.voie != null && x.voie != "")
      if (dsVoie.isEmpty) {
        ""
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

  // Renvoie toutes les informations utiles à un dévoiement.
  // Soit un triplet (Voie, maj de la voie, affiché ou non)
  // A partir de ces informations on est capable de faire tous les autres calculs
  def allDevoimentInfo(
      seqTgaTgd: Seq[TgaTgdInput]): Seq[(String, Int)] = {

    try{

      // Prise en compte du retard pour filtrer les lignes après le départ
      val retard = getCycleRetard(seqTgaTgd)
      val seqWithoutEventAfterDeparture = seqTgaTgd.filter(x => x.maj < x.heure + retard + MARGE_APRES_DEPART_REEL)

      // On doit gérer le cas où les voies sont les mêmes
      // EX : 2 2 2 2 4 4 4 2 2 doit renvoyer deux devoiements 2 -> 4 et 4 -> 2
      // Construction d'un tableau (Voie, Max(Maj),Seq[Maj])
      var dsVoie = seqWithoutEventAfterDeparture
        .sortBy(_.maj)
        .reverse
        .filter(x => x.voie != null && x.voie != "")
        .groupBy(_.voie)
        .map(x => (x._1, x._2.maxBy(_.maj).maj, x._2.map(x => x.maj), x._2.map(x => x.attribut_voie)))
        .toSeq
        .sortBy(_._2)
        .reverse

      // On teste si dans les valeurs de maj il y en a plus petite que la suivante
      // Ce qui veut dire qu'on a l'enchainement 2 2 2 3 3 2 2
      // Auquel cas le maj des premiers voies sera inférieure a celle de la deuxième

      // On répete le split Devoiement 3 fois (max de devoiement = 4 ) pour être sur d'avoir bien extrait toutes les valeures
      val newSeq = splitDevoiement(splitDevoiement(splitDevoiement(dsVoie)))


      newSeq.sortBy(_._2).map(x => (x._1,x._4.filter(x => x != "I").length))

    }
    catch {
      case e: Throwable => {
        // Retour d'une valeur par défaut
        Seq(("",0))
      }
    }
  }

  def splitDevoiement(seqTgaTgd: Seq[(String,Long,Seq[Long],Seq[String])]) : Seq[(String,Long,Seq[Long],Seq[String])] = {

    var newDsVoie  = new ListBuffer[(String,Long,Seq[Long],Seq[String])]()
    for (i <- 0 to seqTgaTgd.length - 1){

      val current = seqTgaTgd(i)

      // Gestion du cas ou on est au bout de la liste
      if(i == seqTgaTgd.length - 1) newDsVoie += current
      else {

        val next = seqTgaTgd(i + 1)

        // Test pour savoir si des éléments sont après
        val eventAfterNext = current._3.filter(x => x < next._2)

        // Si on a des éléments après on modifie dsVoie, on sauvegarde enregistre 2 lignes
        if (eventAfterNext.length > 0) {
          val split1 =  (current._1,current._2,current._3.filter(x => x > next._2),current._4.take(current._3.filter(x => x > next._2).length))
          newDsVoie += split1
          val split2 = (seqTgaTgd(i)._1, eventAfterNext.max, eventAfterNext, current._4.reverse.take(eventAfterNext.length) )
          newDsVoie += split2
        }
        else {
          newDsVoie += current
        }
      }
    }

    newDsVoie.toList.toSeq.sortBy(_._2).reverse
  }

  //Fonction qui renvoie le type du devoiement1 (Affiche ou non Affiché)
  def getTypeDevoiement(seqTgaTgd: Seq[TgaTgdInput], devoiementInfo: Seq[(String,Int)], devoiementNumber: Int): String = {

    try{

      if (devoiementInfo.size <= devoiementNumber) {
        // Valeur par défaut
        ""
      } else {
        val voie_1 = devoiementInfo(devoiementNumber - 1)._1
        val voie_2 = devoiementInfo(devoiementNumber)._1
        val lengthOfNonHidden_Voie = devoiementInfo(devoiementNumber)._2

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

  //Fonction qui retourne la date du dernier affichage voie
  def getDernierAffichage(seqTgaTgd: Seq[TgaTgdInput]): Long = {

    try {
      val etat_train = getEtatTrain(seqTgaTgd)
      // si le train est supperimé le denier
      if (etat_train == "SUP") {
       0
      }
      else {


        val dernier_affichage = seqTgaTgd
          .sortBy(_.maj)
          .filter(x => x.voie != null && x.voie != "")
          .groupBy(_.voie)
          .toSeq
          .map(row => (row._1, row._2.maxBy(_.maj)))
          .sortBy(_._2.maj).reverse.head._2.maj

        dernier_affichage
      }
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


  // Fonction pour gérer les rejets de cycle
  def manageInvalidateCycle(cycleId: String, seqTgaTgd: Seq[TgaTgdInput], rejectReason: String): TgaTgdIntermediate ={

    val cycleIdInv = "INV_" + cycleId
    TgaTgdIntermediate(
      cycleIdInv,
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
  }

}
