package com.sncf.fab.ppiv.persistence
import com.sncf.fab.ppiv.business.{TgaTgdInput, TgaTgdOutput}
import com.sncf.fab.ppiv.pipelineData.TraitementTga
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext


/**
  * Created by simoh-labdoui on 11/05/2017.
  * Service de sauvegarde
  */
object PersistHive extends Serializable {
  /**
    *
    * @param ds sauvegarde le dataset issu des fichiers tga/tgd nettoyés
    */

  def persisteTgaTgdParsedHive(ds: Dataset[TgaTgdInput]): Unit = {

  }

  /**
    * @param df le dataset issu des fichiers TGA TGD et le referentiel des gares
    */
  def persisteQualiteAffichageHive(df: DataFrame, sc : SparkContext): Unit = {


    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    val dfHive = hiveContext.createDataFrame(df.rdd, df.schema)
    // Sauvegarde dans HDFS
    //val hdfsRefineryPath = TraitementTga.getOutputRefineryPath()
    //ds.toDF().write.format("com.datasbricks.spark.csv").save(hdfsRefineryPath)


    dfHive.registerTempTable("dataToSaveHive")

    // Chargement des données de HDFS dans Hive
    hiveContext.sql("CREATE EXTERNAL TABLE IF NOT EXISTS ppiv_ref.iv_tgatgd5 (nom_de_la_gare String, agence String," +
      " segmentation  String, uic String, x String, y String, id_train String, num_train String, type String," +
      " origine_destination String, type_panneau String, dateheure2 String,creneau_horaire String, jour_depart_arrivee INT," +
      " jour_depart_arrivee1 String, affichage_duree1 String, affichage_duree1_minutes String, delai_affichage_voie_sans_retard String, " +
      "duree_temps_affichage String, nb_retard1 INT, dernier_retard_annonce_min INT, nb_retard2 INT, dernier_retard_annonce String, " +
      "affichage_duree2_minutes String, affichage_duree2 String, delai_affichage_voie_avec_retard String, duree_temps_affichage2 String, taux_affichage INT, " +
      "taux_affichage2 INT, affichage_retard String, affichage_duree_retard String, etat_train String, date_affichage_etat_train String, delai_affichage_etat_train_avant_depart_arrivee String," +
      " delai_affichage_etat_train_avant_depart_arrivee_min String, quai_devoiement String, quai_devoiement2 String, quai_devoiement3 String, quai_devoiement4 String," +
      " dernier_quai_affiche String, devoiement INT, devoiement_affiche INT, devoiement_non_affiche INT, type_devoiement String, type_devoiement2 String," +
      " type_devoiement3 String, type_devoiement4 String, carac_devoiement String, date_process String, objectif_h INT)" +
      " row format delimited fields terminated by ','")


    hiveContext.sql("CREATE EXTERNAL TABLE IF NOT EXISTS ppiv_ref.iv_tgatgd6 as select * from dataToSaveHive")

    // Load data to HDFS
    //hiveContext.sql("LOAD DATA INPATH '" + hdfsRefineryPath.replaceAll("hdfs:","") + "' INTO TABLE ppiv_ref.iv_tgatgd3")

    // Affichage pour vérifier que cela a bien marché
    //println("log pour être sur que ca a bien marché")
    //hiveContext.sql("FROM ppiv_ref.iv_tgatgd3 SELECT * LIMIT 10").collect().foreach(println)

    // Problème de out of bounds exception créer la structure finale ca marchera mieux
    //hiveContext.sql("INSERT INTO TABLE ppiv_ref.iv_tgatgd5 select * from dataToSaveHive")

  }

}
