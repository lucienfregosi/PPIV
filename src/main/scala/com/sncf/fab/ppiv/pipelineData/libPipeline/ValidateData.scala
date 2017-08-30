package com.sncf.fab.ppiv.pipelineData.libPipeline

import com.sncf.fab.ppiv.business.TgaTgdInput
import com.sncf.fab.ppiv.utils.AppConf.MARGE_APRES_DEPART_REEL
import org.apache.spark.sql.{Dataset, SQLContext}
import org.joda.time.{DateTime, DateTimeZone}

/**
  * Created by ELFI03951 on 12/07/2017.
  */
object ValidateData {

  def validateField(dsTgaTgd: Dataset[TgaTgdInput], sqlContext : SQLContext): (Dataset[TgaTgdInput],Dataset[TgaTgdInput]) = {
    import sqlContext.implicits._

    // Validation de chaque champ avec les contraintes définies dans le dictionnaire de données
    val currentTimestamp = DateTime.now(DateTimeZone.UTC).getMillis() / 1000


    // Sélection des champs qui répondent à nos spécifications de la donnée
    val dsTgaTgdValidatedFields = dsTgaTgd
      .filter(_.gare matches "^[A-Z]{3}$" )
       .filter(_.train matches  "^[0-2]{0,1}[0-9]$")
      .filter(_.`type` matches "^([A-Z]*)$")
      .filter(_.attribut_voie matches "I|$")
      .filter(_.attribut_voie matches "^(?:[0-9]|[A-Z]|$)$")
      .filter(_.etat matches "^(?:(IND)|(SUP)|(ARR)|$|(\\s))$")
      .filter(_.retard matches  "^(([0-9]{4})|([0-9]{2})|$|\\s)$")

    // Sélection des rejets qui seront enregistrés ailleurs pour analyse
    val dsTgaTgdRejectedFields = dsTgaTgd.filter(x => (x.gare matches("(?!(^[A-Z]{3})$)"))
       ||  (x.train matches  "(?!(^[0-2]{0,1}[0-9]$))")
      ||  (x.`type` matches "(?!(^[A-Z]*$))")
      ||  (x.attribut_voie matches "!(I|$)")
      ||  (x.voie matches "(?!(^(?:[0-9]|[A-Z]|$)$))")
      || (x.etat matches "(?!(^(?:(IND)|(SUP)|(ARR)|$|\\s)$))")
      || (x.retard matches  "(?!(^(?:[0-9]{2}|[0-9]{4}|$|\\s)$))")
    )


    (dsTgaTgdValidatedFields, dsTgaTgdRejectedFields)
  }

  def validateCycle(seqTgaTgdSeq: Seq[TgaTgdInput]): (Boolean, String) = {

    // Validation des cycles. Un cycle doit comporter au moins une voie et tous ses évènements ne peuvent pas se passer x minutes après le départ du train
    // En entrée la liste des évènements pour un cycle id donné.
    // Si la séquence ne contient pas plus d'un cycle on ne valide pas
    // Si la voie est affichée mais elle ne l'est pas lors du départ du train on ne valide pas


    // Décompte des évènements ou la voir est renseignée
    val cntVoieAffiche = seqTgaTgdSeq.filter(x => (x.voie!= null ) && (x.voie!= "")).length

    // Compter le nombre d'évènements après le départ théorique + retard
    val departThéorique = seqTgaTgdSeq(0).heure.toLong

    val retard = BusinessRules.getCycleRetard(seqTgaTgdSeq)

    val departReel = departThéorique + retard.toInt + MARGE_APRES_DEPART_REEL

    val etatTrain = BusinessRules.getEtatTrain(seqTgaTgdSeq)


    // Décompte des évènements se passant après le départ du triain
    val cntEventApresDepart = seqTgaTgdSeq.filter(x=>( x.maj > departReel)).length

    // Si la séquence est composé de moins de deux lignes on ne valide pas on ne pourra rien en faire
    if(seqTgaTgdSeq.length < 2){
      (false, "pasAssezEvent")
    }


    // Si le compte de voie est différent de 0 ou le compte des évènement après la date est égale a la somme des event (= tous les évènements postérieurs à la date de départ du train
    if(cntVoieAffiche != 0 && cntEventApresDepart != seqTgaTgdSeq.length  && etatTrain != "SUP"){
      // On teste si au moment du départ la voie est bien affichée
      try{
        val voieAuMomentDepart = seqTgaTgdSeq.filter(_.maj + retard <= departReel ).sortBy(_.maj).reverse(0).voie
        if(voieAuMomentDepart == ""){
          (false, "PasDeVoieAuMomentDepart")
        }
        else {(true, "ValidCycle")}
      }
    }
    else{
      // Si jamais le train a un état spécial (Supprimé ou retard indéterminé) cela ne veut pas dire qu'il ne faut pas le valider
      // Get de l'état du train. Si IND ou SUP on valide
          if(etatTrain == "SUP" || etatTrain == "IND"){
        (true, "train avec état " + etatTrain)
      }
      else{
        if (cntVoieAffiche == 0 ) {(false,"VoieManquante")}
        else {(false,"EventApresDepart")}
      }
    }
  }

}
