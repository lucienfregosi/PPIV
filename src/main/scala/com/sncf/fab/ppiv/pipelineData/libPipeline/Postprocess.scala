package com.sncf.fab.ppiv.pipelineData.libPipeline

import com.sncf.fab.ppiv.business.{ReferentielGare, TgaTgdInput, TgaTgdOutput, TgaTgdIntermediate, VingPremierChamp, VingtChampsSuivants,NDerniersChamps }
import com.sncf.fab.ppiv.utils.Conversion
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext}


/**
  * Created by ELFI03951 on 12/07/2017.
  */
object Postprocess {
  def saveCleanData(dsToSave: Dataset[TgaTgdInput], sc: SparkContext) : Unit = {
    null
  }

  def joinReferentiel(dsTgaTgd: Dataset[TgaTgdIntermediate], refGares : Dataset[ReferentielGare], sqlContext : SQLContext): DataFrame = {
    // Jointure entre nos données de sorties et le référentiel
    val joinedData = dsTgaTgd.toDF().join(refGares.toDF(), dsTgaTgd.toDF().col("gare") === refGares.toDF().col("TVS"),"leftouter")
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
        BusinessConversion.getDateExtract(row.getLong(7)),
        "",
        "",
        Conversion.unixTimestampToDateTime(row.getLong(5)).toString,
        "",
        0,
        "",
        Conversion.getHHmmss(row.getLong(8))
      )


      val v2 = VingtChampsSuivants(
        "",
        "",
        "",
        0,
        0,
        0,
        Conversion.getHHmmss(row.getLong(9)),
        "",
        Conversion.getHHmmss(row.getLong(10)),
        "",
        "",
        0,
        0,
        Conversion.unixTimestampToDateTime(row.getLong(11)).toString,
        Conversion.getHHmmss(row.getLong(12)),
        row.getString(6),
        Conversion.unixTimestampToDateTime(row.getLong(13)).toString,
        Conversion.getHHmmss(row.getLong(14)),
        ""
      )

      val v3 = NDerniersChamps(
        "",
        "",
        "",
        "",
        row.getString(15),
        0,
        0,
        0,
        row.getString(16),
        row.getString(17),
        row.getString(18),
        row.getString(19),
        "",
        Conversion.unixTimestampToDateTime(row.getLong(20)).toString,
        Conversion.unixTimestampToDateTime(row.getLong(21)).toString,
        0
      )
      TgaTgdOutput(
        v1,
        v2,
        v3,
        "",
        0,
        0,
        0,
        0,
        0,
        0
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
