package com.sncf.fab.ppiv.pipelineData.libPipeline

import com.sncf.fab.ppiv.business.{TgaTgdInput, TgaTgdIntermediate}
import com.sncf.fab.ppiv.spark.batch.TraitementPPIVDriver.LOGGER
import com.sncf.fab.ppiv.utils.Conversion
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.collection.immutable.{ListMap, SortedMap}

/**
  * Created by ELFI03951 on 12/07/2017.
  */
object BusinessRules {


  // Traitement des cycles valides + calcul des KPIs
  def computeBusinessRules (cycleWithEventOver: DataFrame ): RDD[TgaTgdIntermediate] =
  {
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
        TgaTgdInput(split(0), split(1).toLong, split(2), split(3), split(4), split(5), split(6), split(7), split(8), split(9).toLong, split(10), split(11))
      })


      // 6) Validation des cycles
      val isCycleValidated = ValidateData.validateCycle(seqTgaTgd)._1
      if (isCycleValidated == false) {
        val rejectReason = ValidateData.validateCycle(seqTgaTgd)._2
        // TODO : Regarder si il n'a pas un état en IND ou SUP
        // Si oui on enregistre la ligne avec les infos qu'on a
        LOGGER.info("Cycle invalide pour le cycle Id: " + cycleId)
        cycleId = "INV_" + cycleId
        TgaTgdIntermediate(cycleId, seqTgaTgd(0).gare, seqTgaTgd(0).ordes, seqTgaTgd(0).num, seqTgaTgd(0).`type`, seqTgaTgd(0).heure, seqTgaTgd(0).etat, 0, 0, 0, 0, 0, 0, 0, 0, "", "", "", "", rejectReason, 0, 0)
      } else {

        // 7) Nettoyage et mise en forme
        // On se sait pas si on en aura besoin, on la laisse en attendant
        val dataTgaTgdCycleCleaned = Preprocess.cleanCycle(seqTgaTgd)

        // 8) Calcul des différents règles de gestion.
        val premierAffichage = BusinessRules.getPremierAffichage(dataTgaTgdCycleCleaned)
        val affichageDuree1 = BusinessRules.getAffichageDuree1(dataTgaTgdCycleCleaned)
        val affichageDuree2 = BusinessRules.getAffichageDuree2(dataTgaTgdCycleCleaned)
        val dernier_retard_annonce = BusinessRules.getDernierRetardAnnonce(dataTgaTgdCycleCleaned)
        val affichage_retard = BusinessRules.getAffichageRetard(dataTgaTgdCycleCleaned)
        val affichage_duree_retard = BusinessRules.getAffichageDureeRetard(dataTgaTgdCycleCleaned)
        val etat_train = BusinessRules.getEtatTrain(dataTgaTgdCycleCleaned)
        val date_affichage_etat_train = BusinessRules.getDateAffichageEtatTrain(dataTgaTgdCycleCleaned)
        val delai_affichage_etat_train_avant_depart_arrive = BusinessRules.getDelaiAffichageEtatTrainAvantDepartArrive(dataTgaTgdCycleCleaned)
        val dernier_quai_affiche = BusinessRules.getDernierQuaiAffiche(dataTgaTgdCycleCleaned)
        val type_devoiement = BusinessRules.getTypeDevoiement(dataTgaTgdCycleCleaned)(2)
        val type_devoiement2 = BusinessRules.getTypeDevoiement2(dataTgaTgdCycleCleaned)(2)
        val type_devoiement3 = BusinessRules.getTypeDevoiement3(dataTgaTgdCycleCleaned)(2)
        val type_devoiement4 = BusinessRules.getTypeDevoiement4(dataTgaTgdCycleCleaned)(2)
        val dernier_affichage = BusinessRules.getDernierAffichage(dataTgaTgdCycleCleaned)
        val date_process = BusinessRules.getDateProcess(dataTgaTgdCycleCleaned)


        // 9) Création de la classe de sortie sans le référentiel
        TgaTgdIntermediate(cycleId, seqTgaTgd(0).gare, seqTgaTgd(0).ordes, seqTgaTgd(0).num, seqTgaTgd(0).`type`, seqTgaTgd(0).heure, etat_train,
          premierAffichage, affichageDuree1, dernier_retard_annonce, affichageDuree2, affichage_retard, affichage_duree_retard,
          date_affichage_etat_train, delai_affichage_etat_train_avant_depart_arrive, dernier_quai_affiche, type_devoiement, type_devoiement2,
          type_devoiement3, type_devoiement4, dernier_affichage, date_process)
      }
    }
    rddIvTgaTgdWithoutReferentiel
  }

  // Retourne 0 si pas de retard ou le retard dans le cas échéant en secondes
  def getCycleRetard(dsTgaTgd: Seq[TgaTgdInput]) : Long = {

    // Tri sur les horaires d'évènements en croissant puis filtre sur la colonne retard
    val seqFiltered = dsTgaTgd.sortBy(x => x.maj).filter(x => (x.retard !=null) && (x.retard !="") && (x.retard !="0"))

    // Si pas de lignes retournée => pas de retard on revoie 0
    if(seqFiltered.isEmpty){
      0
    } else {

      println("The Sequence filtered is : " )
      val test = seqFiltered (seqFiltered.length - 1)
      println("The Sequence filtered is: -----------------------------------------" + test )
      // Récupération du dernier retard. -1 pour aller chercher sur le dernier index
      val  minuteRetard =  seqFiltered(seqFiltered.length - 1).retard.toLong
      println("minuteRetard : -----------------------------------------" + minuteRetard )
      // Multipliation par 60 pour renvoyer un résultat en secondes
      minuteRetard * 60
    }
  }


  // Fonction qui renvoie la date de premier affichage de la voie en timestamp pour un cycle donné
  def getPremierAffichage(seqTgaTgd: Seq[TgaTgdInput]) : Long = {

    // Récupération de la date de premier affichage. On cherche le moment ou la bonne voie a été affiché pour la première fois
    val dsVoieGrouped = seqTgaTgd.sortBy(_.maj ).reverse.filter(x => x.voie != null && x.voie != "" &&  x.voie   != ("0")).groupBy(_.voie)
   .map{ case(_,group)=> ( group.map(_.maj).min)}
    if (dsVoieGrouped.size == 0){
      0
    }
    else{
      dsVoieGrouped.head
    }
  }

  // Fonction qui renvoie le timestamp durant lequel le train est resté affiché
  def getAffichageDuree1(dsTgaTgdSeq: Seq[TgaTgdInput]) : Long = {

    val departTheorique = dsTgaTgdSeq(0).heure.toLong

    val premierAffichage = getPremierAffichage(dsTgaTgdSeq)

    departTheorique - premierAffichage
  }


  // Fonction qui renvoie le timestamp durant lequel le train est resté affiché retard inclus
  def getAffichageDuree2(dsTgaTgdSeq: Seq[TgaTgdInput]) : Long = {

    val affichageDuree1 = getAffichageDuree1(dsTgaTgdSeq)

    val retard = getCycleRetard(dsTgaTgdSeq)

    affichageDuree1 + retard
  }

  def getDernierRetardAnnonce(dsTgaTgdSeq: Seq[TgaTgdInput]) : Long = {

    val DernierRetardAnnonce = getCycleRetard(dsTgaTgdSeq)
    DernierRetardAnnonce
  }

  def getAffichageRetard(dsTgaTgdSeq: Seq[TgaTgdInput]) : Long = {
    // Tri sur les horaires d'évènements en croissant puis filtre sur la colonne retard
    val seqFiltered = dsTgaTgdSeq.sortBy(x => x.maj).filter(x => (x.retard !=null) && (x.retard !="") && (x.retard !="0"))

    // Si pas de lignes retournée => pas de retard on revoie 0
    if(seqFiltered.isEmpty){
      0
    } else {
      // Récupération du permier  retard.
      val  AffichegeRetard =  seqFiltered(0).maj.toLong
      AffichegeRetard
    }
  }

  def getAffichageDureeRetard(dsTgaTgdSeq: Seq[TgaTgdInput]) : Long = {
    // heure de Depart reelle - getAffichageRetard
    val departTheorique = dsTgaTgdSeq(0).heure.toLong
    val retard          = getCycleRetard(dsTgaTgdSeq)
    val departReel      = (Conversion.unixTimestampToDateTime(departTheorique).plusSeconds(retard.toInt)).getMillis
    val AffichageRetard = getAffichageRetard(dsTgaTgdSeq)
    val AffichageDureeRetard = departReel - AffichageRetard

    AffichageDureeRetard
  }

  def getEtatTrain(dsTgaTgdSeq: Seq[TgaTgdInput]) : String = {
    // si au moins une fois IND alors Ind: Retard Inderminé
    // si une fois SUPP alors Supp: Train supprimé
    val seqFiltered = dsTgaTgdSeq.sortBy(x => x.maj).filter(x => (x.etat !=null) && (x.etat !=""))
    if(seqFiltered.isEmpty){
      null
    } else {
    seqFiltered(0).etat.toString
    }
  }

  def getDateAffichageEtatTrain(dsTgaTgdSeq: Seq[TgaTgdInput]) : Long = {
    val seqFiltered = dsTgaTgdSeq.sortBy(x => x.maj).filter(x => (x.etat !=null) && (x.etat !=""))
    if(seqFiltered.isEmpty){
      0
    } else {
      seqFiltered(0).maj.toLong
    }
  }

  def getDelaiAffichageEtatTrainAvantDepartArrive(dsTgaTgdSeq: Seq[TgaTgdInput]) : Long = {
    val departTheorique = dsTgaTgdSeq(0).heure.toLong
    val DateAffichageEtatTrain = getDateAffichageEtatTrain(dsTgaTgdSeq)
    if (DateAffichageEtatTrain!= 0) {
      departTheorique - DateAffichageEtatTrain
    } else
    {
      0
    }
  }

  def getDernierQuaiAffiche(dsTgaTgdSeq: Seq[TgaTgdInput]) : String = {

    val dsVoie = dsTgaTgdSeq.sortBy(_.maj ).reverse.filter(x => x.voie != null && x.voie != "" &&  x.voie   != ("0"))
    if(dsVoie.isEmpty){
      null
    } else {
      dsVoie(0).voie.toString
    }
  }


  def allDevoimentInfo (dsTgaTgdSeq: Seq[TgaTgdInput]) : Seq[(String,TgaTgdInput,Int )]={
    val dsVoie = dsTgaTgdSeq
      .sortBy(_.maj).filter(x => x.voie != null && x.voie != "" &&  x.voie   != ("0"))
      .groupBy(_.voie)

      val dvInfo = dsVoie.toSeq.map(row=> (row._1, row._2.maxBy(_.maj),row._2.filter(_.attribut_voie != "I").length )).sortBy(_._2.maj)
     dvInfo
  }
  def getTypeDevoiement(dsTgaTgdSeq: Seq[TgaTgdInput]) :  Seq[String]  = {

  val dvInfo = allDevoimentInfo(dsTgaTgdSeq)
    if  (dvInfo.size <=1){
      Seq("NO DEV", "NO DEV", "NO DEV")
    }
    else {
      val Voie_1 = dvInfo(0)._2.voie
      val Voie_2 = dvInfo(1)._2.voie
      val lengthOfNonHidden_Voie = dvInfo(1)._3

      if ( lengthOfNonHidden_Voie >=1){
        Seq( Voie_1,Voie_2,"Affiche")
      } else {
        Seq( Voie_1,Voie_2,"Non_Affiche")
        }
      }
  }

  def getTypeDevoiement2(dsTgaTgdSeq: Seq[TgaTgdInput]) :  Seq[String]  = {
    val dvInfo = allDevoimentInfo(dsTgaTgdSeq)
    if  (dvInfo.size <=2){
      Seq("NO DEV", "NO DEV", "NO DEV")
    }
    else {
      val Voie_1 = dvInfo(1)._2.voie
      val Voie_2 = dvInfo(2)._2.voie
      val lengthOfNonHidden_Voie = dvInfo(2)._3

      if ( lengthOfNonHidden_Voie >=1){
        Seq( Voie_1,Voie_2,"Affiche")
      } else {
        Seq( Voie_1,Voie_2,"Non_Affiche")
      }
    }
  }

  def getTypeDevoiement3(dsTgaTgdSeq: Seq[TgaTgdInput]) : Seq[String] = {
    val dvInfo = allDevoimentInfo(dsTgaTgdSeq)
    if  (dvInfo.size <=3){
      Seq("NO DEV", "NO DEV", "NO DEV")
    }
    else {
      val Voie_1 = dvInfo(2)._2.voie
      val Voie_2 = dvInfo(3)._2.voie
      val lengthOfNonHidden_Voie = dvInfo(3)._3

      if ( lengthOfNonHidden_Voie >=1){
        Seq( Voie_1,Voie_2,"Affiche")
      } else {
        Seq( Voie_1,Voie_2,"Non_Affiche")
      }
    }
  }

  def getTypeDevoiement4(dsTgaTgdSeq: Seq[TgaTgdInput]) : Seq[String] = {
    val dvInfo = allDevoimentInfo(dsTgaTgdSeq)
    if  (dvInfo.size <=4){
      Seq("NO DEV", "NO DEV", "NO DEV")
    }
    else {
      val Voie_1 = dvInfo(3)._2.voie
      val Voie_2 = dvInfo(4)._2.voie
      val lengthOfNonHidden_Voie = dvInfo(4)._3

      if ( lengthOfNonHidden_Voie >=1){
        Seq( Voie_1,Voie_2,"Affiche")
      } else {
        Seq( Voie_1,Voie_2,"Non_Affiche")
      }
    }
  }

  def getDernierAffichage(dsTgaTgdSeq: Seq[TgaTgdInput]) : Long = {

    val dsVoieGrouped = dsTgaTgdSeq.sortBy(_.maj ).reverse.filter(x => x.voie != null && x.voie != "" &&  x.voie   != ("0")).groupBy(_.voie).map{ case(_,group)=> ( group.map(_.maj).max)}
    if (dsVoieGrouped.size == 0){
      0
    }
    else{
      dsVoieGrouped.last
    }
  }

  def getDateProcess(dsTgaTgdSeq: Seq[TgaTgdInput]) : Long = {
    5
  }


}
