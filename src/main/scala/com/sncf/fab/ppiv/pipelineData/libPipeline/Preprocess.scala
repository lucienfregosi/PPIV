package com.sncf.fab.ppiv.pipelineData.libPipeline

import com.sncf.fab.ppiv.business.TgaTgdInput
import org.apache.spark.sql.{Dataset, SQLContext}
import org.joda.time.DateTime

/**
  * Created by ELFI03951 on 12/07/2017.
  */
object Preprocess {
  def applyStickingPlaster(dsTgaTgd: Dataset[TgaTgdInput], sqlContext : SQLContext): Dataset[TgaTgdInput] = {
    import sqlContext.implicits._

    // Application du sparadrap
    // Pour les trains passe nuit, affiché entre après 18h et partant le lendemain il y a un bug connu et identifié dans OBIER
    // Les évènements de 18à 24h seront a la date N+1. Il faut donc leur retrancher un jour pour la cohérence

    // Si maj > 18 && heure < 12 on retranche un jour a la date de maj

    val dsTgaTgdWithStickingPlaster = dsTgaTgd.map{
      row =>
        // Get de l'heure de départ du train (hourHeure) et de l'heure de l'évènement (hourMaj)
        val hourMaj    = new DateTime(row.maj).toDateTime.toString("hh").toInt
        val hourHeure  = new DateTime(row.heure).toDateTime.toString("hh").toInt

        // Test si on est dans les conditions d'application du sparadrap
        val newMaj = if(hourMaj > 18 && hourHeure < 12){
          // Si oui On retranche un jour
          new DateTime(row.maj).plusDays(-1).getMillis / 1000
        } else row.maj

        // Création d'un nouveau TgaTgdInput avec la nouvelle valeur si le test a été passé
        TgaTgdInput(row.gare, newMaj.asInstanceOf[java.lang.Long], row.train, row.ordes, row.num,row.`type`, row.picto, row.attribut_voie, row.voie, row.heure, row.etat, row.retard)
    }
    dsTgaTgdWithStickingPlaster
  }
  def cleanCycle(seqTgaTgd: Seq[TgaTgdInput]): Seq[TgaTgdInput] = {
    // Nettoyage, mise en forme des lignes, conversion des heures etc ..
    seqTgaTgd
  }
}
