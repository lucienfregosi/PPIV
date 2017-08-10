package com.sncf.fab.ppiv.pipelineData.libPipeline

import java.util.concurrent.TimeUnit

import com.sncf.fab.ppiv.business.{NDerniersChamps, ReferentielGare, TgaTgdInput, TgaTgdIntermediate, TgaTgdOutput, VingPremierChamp, VingtChampsSuivants}
import com.sncf.fab.ppiv.utils.Conversion
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext}


/**
  * Created by ELFI03951 on 12/07/2017.
  */
object Postprocess {

 def postprocess (dsTgaTgd: Dataset[TgaTgdIntermediate], refGares : Dataset[ReferentielGare], sqlContext : SQLContext, Panneau: String):DataFrame = {

   // Jointure avec le référentiel
   // Nettoyage du référentiel pour enlever les TVS vide
   val cleanedRefGares = refGares.filter(x=> (x.TVS != null && x.TVS != "" ) ).distinct
   val dataTgaTgdWithReferentiel = Postprocess.joinReferentiel(dsTgaTgd, cleanedRefGares, sqlContext)

 // Inscription dans la classe finale TgaTgdOutput avec conversion et formatage
   val dataTgaTgdOutput = Postprocess.formatTgaTgdOuput(dataTgaTgdWithReferentiel, sqlContext, Panneau)
   dataTgaTgdOutput
  }

  def saveCleanData(dsToSave: Dataset[TgaTgdInput], sc: SparkContext) : Unit = {
    null
  }

  def joinReferentiel(dsTgaTgd: Dataset[TgaTgdIntermediate], refGares : Dataset[ReferentielGare], sqlContext : SQLContext): DataFrame = {
    // Jointure entre nos données de sorties et le référentiel
    val joinedData = dsTgaTgd.toDF().join(refGares.toDF(), dsTgaTgd.toDF().col("gare") === refGares.toDF().col("TVS"),"inner")
    joinedData
  }

  def formatTgaTgdOuput(dfTgaTgd: DataFrame, sqlContext : SQLContext, panneau: String) : DataFrame = {
    import sqlContext.implicits._
    
    val affichageFinal =  dfTgaTgd.map(row => {
      val v1 = VingPremierChamp(
        row.getString(23),
        row.getString(34),
        row.getString(25),
        row.getString(26),
        BusinessConversion.getFloat(row.getString(37)),
        BusinessConversion.getFloat(row.getString(38)),
        row.getString(0),
        row.getString(3),
        row.getString(4),
        row.getString(2),
        panneau,
        Conversion.unixTimestampToDateTime(row.getLong(7)).toString,
        BusinessConversion.getDateExtract(row.getLong(20)),
        BusinessConversion.getMois(row.getLong(20)),
        BusinessConversion.getAnnee(row.getLong(20)),
        Conversion.unixTimestampToDateTime(row.getLong(5)).toString,
        BusinessConversion.getCreneau_horaire(row.getLong(5)),
        BusinessConversion.getNumberoftheday(row.getLong(5)),
        BusinessConversion.getThreeFirstLettersOftheday(row.getLong(5)) ,
        Conversion.getHHmmssFromMillis(row.getLong(8))
      )


      val v2 = VingtChampsSuivants(
        TimeUnit.MILLISECONDS.toMinutes(row.getLong(8) * 1000 ).toString,
        BusinessConversion.getDelai_affichage_voie_sans_retard (row.getLong(8)),
        BusinessConversion.getDuree_temps_affichage(row.getLong(8)),
        BusinessConversion.getNbretard1(row.getLong(9)),
        BusinessConversion.getDernier_retard_annonce_min(row.getLong(9)),
        BusinessConversion.getNbretard2(row.getLong(9)),
        Conversion.getHHmmssFromMillis(row.getLong(9)) ,
        TimeUnit.MILLISECONDS.toMinutes(row.getLong(10) * 1000 ).toString,
        Conversion.getHHmmssFromMillis(row.getLong(10)),
        BusinessConversion.getDelai_affichage_voie_avec_retard(row.getLong(10)),
        BusinessConversion.getDuree_temps_affichage2(row.getLong(10)),
        BusinessConversion.getTauxAffichage(row.getLong(8)),
        BusinessConversion.getTauxAffichage(row.getLong(10)),
        BusinessConversion.getAffichageRetard(row.getLong(11)),
        BusinessConversion.getAffichage_duree_retard (row.getLong(12)),
        row.getString(6),
        BusinessConversion.getDateAffichageEtatTrain(row.getLong(13)),
        BusinessConversion.getDelai_affichage_etat_train_avant_depart_arrive(row.getLong(14)),
        TimeUnit.MILLISECONDS.toMinutes(row.getLong(14) * 1000 ).toString
      )

      val v3 = NDerniersChamps(
        BusinessConversion.getQuaiDevoiement(row.getString(16)),
        BusinessConversion.getQuaiDevoiement(row.getString(17)),
        BusinessConversion.getQuaiDevoiement(row.getString(18)),
        BusinessConversion.getQuaiDevoiement(row.getString(19)),
        row.getString(15),
        BusinessConversion.getNbTotaldevoiement(row.getString(16), row.getString(17), row.getString(18),row.getString(19)),
        BusinessConversion.getNbDevoiement_affiche (row.getString(16), row.getString(17), row.getString(18),row.getString(19)) ,
        BusinessConversion.getNvDevoiement_non_affiche (row.getString(16), row.getString(17), row.getString(18),row.getString(19)),
        BusinessConversion.getTypeDevoiement(row.getString(16)),
        BusinessConversion.getTypeDevoiement(row.getString(17)),
        BusinessConversion.getTypeDevoiement(row.getString(18)),
        BusinessConversion.getTypeDevoiement(row.getString(19)),
        BusinessConversion.getCracDevoiement (row.getString(16), row.getString(17), row.getString(18),row.getString(19)),
        Conversion.unixTimestampToDateTime(row.getLong(20)).toString,
        Conversion.unixTimestampToDateTime(row.getLong(21)).toString,
        BusinessConversion.getAffichage_duree_retard_minutes (row.getLong(12))
      )
      TgaTgdOutput(
        v1,
        v2,
        v3,
        BusinessConversion.getDelai_affichage_duree_retard(row.getLong(12)),
        BusinessConversion.geTaux_affichage_30 (row.getLong(8)),
        BusinessConversion.geTaux_affichage_30(row.getLong(10)),
        BusinessConversion.geTaux_affichage_45 (row.getLong(8)),
        BusinessConversion.geTaux_affichage_45 (row.getLong(10)),
        BusinessConversion.geTaux_affichage_15 (row.getLong(8)),
        BusinessConversion.geTaux_affichage_15 (row.getLong(10))
      )
    })

    val dfFinal = affichageFinal.toDF().select(
      "first.nom_de_la_gare",
      "first.agence",
      "first.segmentation",
      "first.uic",
      "first.x",
      "first.y",
      "first.id_train",
      "first.num_train",
      "first.type",
      "first.origine_destination",
      "first.type_panneau",
      "first.premier_affichage",
      "first.date_extract",
      "first.mois",
      "first.annee",
      "first.dateheure2",
      "first.creneau_horaire",
      "first.jour_départ_arrivée",
      "first.jour_départ_arrivée1",
      "first.affichage_durée1",
      "second.affichage_durée1_minutes",
      "second.delai_affichage_voie_sans_retard",
      "second.duree_temps_affichage",
      "second.nb_retard1",
      "second.dernier_retard_annonce_min",
      "second.nb_retard2",
      "second.dernier_retard_annonce",
      "second.affichage_duree_2minutes",
      "second.affichage_duree_2",
      "second.delai_affichage_voie_avec_retard",
      "second.duree_temps_affichage2",
      "second.taux_affichage",
      "second.taux_affichage2",
      "second.affichage_retard",
      "second.affichage_duree_retard",
      "second.etat_train",
      "second.date_affichage_etat_train",
      "second.delai_affichage_etat_train_avant_depart_arrive",
      "second.delai_affichage_etat_train_avant_depart_arrive_min",
      "third.quai_devoiement",
      "third.quai_devoiement2",
      "third.quai_devoiement3",
      "third.quai_devoiement4",
      "third.dernier_quai_affiche",
      "third.devoiement",
      "third.devoiement_affiche",
      "third.devoiement_non_affiche",
      "third.type_devoiement",
      "third.type_devoiement2",
      "third.type_devoiement3",
      "third.type_devoiement4",
      "third.carac_devoiement",
      "third.dernier_affichage",
      "third.date_process",
      "third.affichage_duree_retard_minutes",
      "delai_affichage_duree_retard",
      "taux_affichage_30",
      "taux_affichage2_30",
      "taux_affichage_45",
      "taux_affichage2_45",
      "taux_affichage_15",
      "taux_affichage2_15"
    )
    dfFinal
  }
}
