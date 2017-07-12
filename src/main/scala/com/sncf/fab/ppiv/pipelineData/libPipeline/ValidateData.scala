package com.sncf.fab.ppiv.pipelineData.libPipeline

import com.sncf.fab.ppiv.business.TgaTgdInput
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
      .filter(_.`type` matches "^([A-Z]+)$")
      .filter(_.attribut_voie matches "I|$")
      .filter(_.attribut_voie matches "^(?:[0-9]|[A-Z]|$)$")
      .filter(_.etat matches "^(?:(IND)|(SUP)|(ARR)|$|(\\s))$")
      .filter(_.retard matches  "^(([0-9]{4})|([0-9]{2})|$|\\s)$")

    // Sélection des rejets qui seront enregistrés ailleurs pour analyse
    val dsTgaTgdRejectedFields = dsTgaTgd.filter(x => (x.gare matches("(?!(^[A-Z]{3})$)")) || (x.maj > currentTimestamp)
      ||  (x.train matches  "(?!(^[0-2]{0,1}[0-9]$))")
      ||  (x.`type` matches "(?!(^[A-Z]+$))")
      ||  (x.attribut_voie matches "!(I|$)")
      ||  (x.voie matches "(?!(^(?:[0-9]|[A-Z]|$)$))")
      || (x.etat matches "(?!(^(?:(IND)|(SUP)|(ARR)|$|\\s)$))")
      || (x.retard matches  "(?!(^(?:[0-9]{2}|[0-9]{4}|$|\\s)$))")
    )

    (dsTgaTgdValidatedFields, dsTgaTgdRejectedFields)
  }

}
