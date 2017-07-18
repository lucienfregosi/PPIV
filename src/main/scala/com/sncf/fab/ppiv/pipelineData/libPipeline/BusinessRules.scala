package com.sncf.fab.ppiv.pipelineData.libPipeline

import com.sncf.fab.ppiv.business.TgaTgdInput

/**
  * Created by ELFI03951 on 12/07/2017.
  */
object BusinessRules {
  // Retourne 0 si pas de retard ou le retard dans le cas échéant en secondes
  def getCycleRetard(dsTgaTgd: Seq[TgaTgdInput]) : Long = {

    // Tri sur les horaires d'évènements en croissant puis filtre sur la colonne retard
    val seqFiltered = dsTgaTgd.sortBy(x => x.maj).filter(x => (x.retard !=null) && (x.retard !="") && (x.retard !="0"))

    // Si pas de lignes retournée => pas de retard on revoie 0
    if(seqFiltered.isEmpty){
      0
    } else {
      // Récupération du dernier retard. -1 pour aller chercher sur le dernier index
      val  minuteRetard =  seqFiltered(seqFiltered.length - 1).retard.toLong
      // Multipliation par 60 pour renvoyer un résultat en secondes
      minuteRetard * 60
    }
  }


  // Fonction qui renvoie la date de premier affichage de la voie en timestamp pour un cycle donné
  def getPremierAffichage(seqTgaTgd: Seq[TgaTgdInput]) : Long = {

    // Récupération de la date de premier affichage. On cherche le moment ou la bonne voie a été affiché pour la première fois
    val dsVoieGrouped = seqTgaTgd.sortBy(_.maj ).reverse.filter(x => x.voie != null && x.voie != "" &&  x.voie   != ("0")).groupBy(_.voie).map{ case(_,group)=> ( group.map(_.maj).min)}
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
    5
  }

  def getAffichageRetard(dsTgaTgdSeq: Seq[TgaTgdInput]) : Long = {
    5
  }

  def getAffichageDureeRetard(dsTgaTgdSeq: Seq[TgaTgdInput]) : Long = {
    5
  }

  def getEtatTrain(dsTgaTgdSeq: Seq[TgaTgdInput]) : String = {
    "TO_COMPUTE"
  }

  def getDateAffichageEtatTrain(dsTgaTgdSeq: Seq[TgaTgdInput]) : Long = {
    5
  }

  def getDelaiAffichageEtatTrainAvantDepartArrive(dsTgaTgdSeq: Seq[TgaTgdInput]) : Long = {
    5
  }

  def getDernierQuaiAffiche(dsTgaTgdSeq: Seq[TgaTgdInput]) : String = {
    "TO_COMPUTE"
  }

  def getTypeDevoiement(dsTgaTgdSeq: Seq[TgaTgdInput]) : String = {
    "TO_COMPUTE"
  }

  def getTypeDevoiement2(dsTgaTgdSeq: Seq[TgaTgdInput]) : String = {
    "TO_COMPUTE"
  }

  def getTypeDevoiement3(dsTgaTgdSeq: Seq[TgaTgdInput]) : String = {
    "TO_COMPUTE"
  }

  def getTypeDevoiement4(dsTgaTgdSeq: Seq[TgaTgdInput]) : String = {
    "TO_COMPUTE"
  }

  def getDernierAffichage(dsTgaTgdSeq: Seq[TgaTgdInput]) : Long = {

    // Récupération de la date du dernier affichage. On cherche le moment ou la bonne voie a été affiché pour la dernière fois
    val dsVoieGrouped = seqTgaTgd.sortBy(_.maj ).reverse.filter(x => x.voie != null && x.voie != "" &&  x.voie   != ("0")).groupBy(_.voie).map{ case(_,group)=> ( group.map(_.maj).max)}
    if (dsVoieGrouped.size == 0){
      0
    }
    else{
      dsVoieGrouped.head
    }
  }

  def getDateProcess(dsTgaTgdSeq: Seq[TgaTgdInput]) : Long = {
    5
  }


}
