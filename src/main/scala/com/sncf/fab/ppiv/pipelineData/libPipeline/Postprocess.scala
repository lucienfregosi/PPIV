package com.sncf.fab.ppiv.pipelineData.libPipeline

import com.sncf.fab.ppiv.business.{ReferentielGare, TgaTgdInput, TgaTgdOutput, TgaTgdWithoutRef, VingPremierChamp, VingtChampsSuivants,NDerniersChamps }
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

  def joinReferentiel(dsTgaTgd: Dataset[TgaTgdWithoutRef],  refGares : Dataset[ReferentielGare], sqlContext : SQLContext): DataFrame = {
    // Jointure entre nos données de sorties et le référentiel
    val joinedData = dsTgaTgd.toDF().join(refGares.toDF(), dsTgaTgd.toDF().col("gare") === refGares.toDF().col("TVS"))

    joinedData
  }

  def formatTgaTgdOuput(dfTgaTgd: DataFrame, sqlContext : SQLContext, panneau: String) : DataFrame = {
    import sqlContext.implicits._


    val affichageFinal =  dfTgaTgd.map(row => {
      val v1 = VingPremierChamp(
        row.getString(11),
        row.getString(22),
        row.getString(15),
        row.getString(13),
        row.getString(25),
        row.getString(26),
        row.getString(0),
        row.getString(3),
        row.getString(4),
        row.getString(2),
        panneau,
        "",
        "",
        "",
        "",
        "",
        "",
        0,
        "",
        ""
      )

      val v2 = VingtChampsSuivants(
        "",
        "",
        "",
        0,
        0,
        0,
        "",
        "",
        "",
        "",
        "",
        0,
        0,
        "",
        "",
        "",
        "",
        "",
        ""
      )

      val v3 = NDerniersChamps(
        "",
        "",
        "",
        "",
        "",
        0,
        0,
        0,
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        0
      )
      TgaTgdOutput(
        v1,
        v2,
        v3,
        "",
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
      "v1.agence",
      "v1.nom_de_la_gare",
      "v1.agence",
      "v1.segmentation",
      "v1.uic",
      "v1.x",
      "v1.y",
      "v1.id_train",
      "v1.num_train",
      "v1.type",
      "v1.origine_destination",
      "v1.type_panneau",
      "v1.premier_affichage",
      "v1.date_extract",
      "v1.mois",
      "v1.annee",
      "v1.dateheure2",
      "v1.creneau_horaire",
      "v1.jour_départ_arrivée",
      "v1.jour_départ_arrivée1",
      "v1.affichage_durée1",
      "v2.affichage_durée1_minutes",
      "v2.delai_affichage_voie_sans_retard",
      "v2.duree_temps_affichage",
      "v2.nb_retard1",
      "v2.dernier_retard_annonce_min",
      "v2.nb_retard2",
      "v2.dernier_retard_annonce",
      "v2.affichage_duree_2minutes",
      "v2.affichage_duree_2",
      "v2.delai_affichage_voie_avec_retard",
      "v2.duree_temps_affichage2",
      "v2.taux_affichage",
      "v2.taux_affichage2",
      "v2.affichage_retard",
      "v2.affichage_duree_retard",
      "v2.etat_train",
      "v2.date_affichage_etat_train",
      "v2.delai_affichage_etat_train_avant_depart_arrive",
      "v2.delai_affichage_etat_train_avant_depart_arrive_min",
      "v3.quai_devoiement",
      "v3.quai_devoiement2",
      "v3.quai_devoiement3",
      "v3.quai_devoiement4",
      "v3.dernier_quai_affiche",
      "v3.devoiement",
      "v3.devoiement_affiche",
      "v3.devoiement_non_affiche",
      "v3.type_devoiement",
      "v3.type_devoiement2",
      "v3.type_devoiement3",
      "v3.type_devoiement4",
      "v3.carac_devoiement",
      "v3.dernier_affichage",
      "v3.date_process",
      "v3.affichage_duree_retard_minutes",
      "delai_affichage_duree_retard",
      "duree_temps_affichage2",
      "taux_affichage_30",
      "taux_affichage2_30",
      "taux_affichage_45",
      "taux_affichage2_45",
      "taux_affichage_15",
      "taux_affichage2_15"
    )
    dfFinal.show()

    dfFinal


  }
}
