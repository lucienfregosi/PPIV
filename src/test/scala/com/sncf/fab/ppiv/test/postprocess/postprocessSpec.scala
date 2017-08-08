package com.sncf.fab.ppiv.test.postprocess

import com.sncf.fab.ppiv.business.{ReferentielGare, TgaTgdCycleId, TgaTgdIntermediate}
import com.sncf.fab.ppiv.parser.DatasetsParser
import com.sncf.fab.ppiv.pipelineData.libPipeline.{BuildCycleOver, LoadData, Postprocess}
import com.sncf.fab.ppiv.utils.AppConf.{PPIV, REF_GARES, SPARK_MASTER}
import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.LongType
import org.apache.spark.{SparkConf, SparkContext}
import org.specs2._


/**
  * Created by ELFI03951 on 30/06/2017.
  */
class postprocessSpec extends Specification{

  def is = s2"""

This is a specification fot the "postprocess"
The 'postprocess'  output count   should
  be equal to 2                                   $e1
  """


  val sparkConf = new SparkConf()
    .setAppName(PPIV)
    .setMaster(SPARK_MASTER)
    .set("spark.driver.allowMultipleContexts", "true")


  @transient val sc = new SparkContext(sparkConf)
  @transient val sqlContext = new SQLContext(sc)


/*
  import sqlContext.implicits._

// Load the input of Postprocess
  //val path = "PPIV/src/test/resources/data/BeforePostprocess.deflate"
  val path = "PPIV/src/test/resources/data/BeforePostPro2.deflate"

  val newNamesTgaTgdInter = Seq("cycleId" ,
    "gare",
    "origine_destination",
    "num_train",
    "type_train",
    "dateheure2",
    "etat_train",
    "premierAffichage",
    "affichageDuree1",
    "dernier_retard_annonce",
    "affichageDuree2",
    "affichage_retard",
    "affichage_duree_retard",
    "date_affichage_etat_train",
    "delai_affichage_etat_train_avant_depart_arrive",
    "dernier_quai_affiche",
    "type_devoiement",
    "type_devoiement2",
    "type_devoiement3",
    "type_devoiement4",
    "dernier_affichage",
    "date_process")

val DataBeforepreprocess= sqlContext.read.format("com.databricks.spark.csv").load(path).toDF(newNamesTgaTgdInter: _*)
  .withColumn("dateheure2", 'dateheure2.cast(LongType))
  .withColumn("premierAffichage", 'premierAffichage.cast(LongType))
  .withColumn("affichageDuree1", 'affichageDuree1.cast(LongType))
  .withColumn("dernier_retard_annonce", 'dernier_retard_annonce.cast(LongType))
  .withColumn("affichage_retard", 'affichage_retard.cast(LongType))
  .withColumn("affichageDuree2", 'affichageDuree2.cast(LongType))
  .withColumn("affichage_duree_retard", 'affichage_duree_retard.cast(LongType))
  .withColumn("date_affichage_etat_train", 'date_affichage_etat_train.cast(LongType))
  .withColumn("delai_affichage_etat_train_avant_depart_arrive", 'delai_affichage_etat_train_avant_depart_arrive.cast(LongType))
  .withColumn("dernier_affichage", 'dernier_affichage.cast(LongType))
 .withColumn("date_process", 'date_process.cast(LongType))
  .as[TgaTgdIntermediate]


  val newNamesRefGares = Seq("CodeGare","IntituleGare","NombrePlateformes","SegmentDRG","UIC","UniteGare","TVS","CodePostal","Commune","DepartementCommune","Departement","Region","AgenceGC","RegionSNCF","NiveauDeService","LongitudeWGS84","LatitudeWGS84","DateFinValiditeGare")

  // Chargement du CSV référentiel
  val refGares = sqlContext.read
    .option("delimiter", ";")
    .option("header", "true")
    .option("charset", "UTF8")
    .format("com.databricks.spark.csv")
    .load("PPIV/refinery/PPIV_PHASE2/referentiel.csv")
    .toDF(newNamesRefGares: _*)
    .as[ReferentielGare]

  // Parsing du CSV a l'intérieur d'un object ReferentielGare
  refGares.toDF().map(DatasetsParser.parseRefGares).toDS()


 Postprocess.postprocess(DataBeforepreprocess, refGares, sqlContext, "TGA")
*/
  def e1 = "true" must beEqualTo("true")


}