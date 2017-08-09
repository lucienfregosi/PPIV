package com.sncf.fab.ppiv.pipelineData.libPipeline

import com.sncf.fab.ppiv.business.TgaTgdInput
import com.sncf.fab.ppiv.utils.Conversion
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
      .filter(_.maj <= currentTimestamp)
      .filter(_.train matches  "^[0-2]{0,1}[0-9]$")
      .filter(_.`type` matches "^([A-Z]*)$")
      .filter(_.attribut_voie matches "I|$")
      .filter(_.attribut_voie matches "^(?:[0-9]|[A-Z]|$)$")
      .filter(_.etat matches "^(?:(IND)|(SUP)|(ARR)|$|(\\s))$")
      .filter(_.retard matches  "^(([0-9]{4})|([0-9]{2})|$|\\s)$")

    // Sélection des rejets qui seront enregistrés ailleurs pour analyse
    val dsTgaTgdRejectedFields = dsTgaTgd.filter(x => (x.gare matches("(?!(^[A-Z]{3})$)")) || (x.maj > currentTimestamp)
      ||  (x.train matches  "(?!(^[0-2]{0,1}[0-9]$))")
      ||  (x.`type` matches "(?!(^[A-Z]*$))")
      ||  (x.attribut_voie matches "!(I|$)")
      ||  (x.voie matches "(?!(^(?:[0-9]|[A-Z]|$)$))")
      || (x.etat matches "(?!(^(?:(IND)|(SUP)|(ARR)|$|\\s)$))")
      || (x.retard matches  "(?!(^(?:[0-9]{2}|[0-9]{4}|$|\\s)$))")
    )


    (dsTgaTgdValidatedFields, dsTgaTgdRejectedFields)
  }

  def validateCycle(dsTgaTgdSeq: Seq[TgaTgdInput]): (Boolean, String) = {

    // Validation des cycles. Un cycle doit comporter au moins une voie et tous ses évènements ne peuvent pas se passer x minutes après le départ du train
    // En entrée la liste des évènements pour un cycle id donné.

    // Décompte des évènements ou la voir est renseignée
    val cntVoieAffiche = dsTgaTgdSeq.filter(x => (x.voie!= null ) && (x.voie!= "0")&& (x.voie!= "")).length

    // Compter le nombre d'évènements après le départ théorique + retard
    val departThéorique = dsTgaTgdSeq(0).heure.toLong

    val retard = BusinessRules.getCycleRetard(dsTgaTgdSeq)
    // 10 minutes : pour la marge d'erreur imposé par le métier
    val margeErreur = 10 * 60


   // val departReel = departThéorique + retard + margeErreur
   val departReel = (Conversion.unixTimestampToDateTime(departThéorique).plusSeconds(retard.toInt).plusMinutes(10).getMillis)/1000


    // Décompte des évènements se passant après le départ du triain
    val cntEventApresDepart = dsTgaTgdSeq.filter(x=>( x.maj > departReel)).length

    // Si le compte de voie est différent de 0 ou le compte des évènement après la date est égale a la somme des event (= tous les évènements postérieurs à la date de départ du train
    if(cntVoieAffiche != 0 && cntEventApresDepart != dsTgaTgdSeq.length ){
      (true, "ValidCycle")
    }
    else{
      if (cntVoieAffiche == 0 ) {(false,"Voie")}
      else {(false,"EventApresDepart")}
    }
  }

}