package com.sncf.fab.ppiv.pipelineData.libPipeline

import com.sncf.fab.ppiv.business.TgaTgdInput
import com.sncf.fab.ppiv.utils.Conversion

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

  def getTypeDevoiement(dsTgaTgdSeq: Seq[TgaTgdInput]) : String = {

    val dsVoie = dsTgaTgdSeq.filter(x => x.voie != null && x.voie != "" &&  x.voie   != ("0")).sortBy(_.maj).groupBy(_.voie)

    dsVoie.foreach(println)

    val desVoieWithAttVoie = dsVoie.map{ case(_,group)=> ( group.map(_.attribut_voie).contains("I"))}

    println ("Test Type devoiement ")
    desVoieWithAttVoie.foreach(println)


    if  (desVoieWithAttVoie.size <=1){
      null
    }
    else {

      val BoolFirstDevoiement2 = desVoieWithAttVoie.drop(1).take(1).toList.headOption

      println ("BoolFirstDevoiement2 :" + BoolFirstDevoiement2)
      //val typeFirstDev = dsVoie.slice(1,1).toList.headOption
      if ( BoolFirstDevoiement2 == false){
        "Affiche"
      } else {
        "Non_Affiche"
        }
      }
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
