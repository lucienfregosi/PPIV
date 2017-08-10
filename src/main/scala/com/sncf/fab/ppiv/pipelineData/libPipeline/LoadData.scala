package com.sncf.fab.ppiv.pipelineData.libPipeline

import com.sncf.fab.ppiv.business.{ReferentielGare, TgaTgdInput}
import com.sncf.fab.ppiv.parser.DatasetsParser
import com.sncf.fab.ppiv.utils.AppConf.REF_GARES
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{Dataset, SQLContext}

/**
  * Created by ELFI03951 on 12/07/2017.
  */
object LoadData {
  def loadTgaTgd(sqlContext : SQLContext, path: String): Dataset[TgaTgdInput] = {
    import sqlContext.implicits._

    // Définition du nom de chacune des colonnes car on recoit les fichiers sans headers
    val newNamesTgaTgd = Seq("gare","maj","train","ordes","num","type","picto","attribut_voie","voie","heure","etat","retard","null")

    // Lecture du CSV avec les bons noms de champs
    val dsTgaTgd = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("delimiter", ";")
      .load(path).toDF(newNamesTgaTgd: _*)
      .withColumn("maj", 'maj.cast(LongType))
      .withColumn("heure", 'heure.cast(LongType))
      .as[TgaTgdInput]

    // Parsing du CSV a l'intérieur d'un object TgaTgaInput
    dsTgaTgd.toDF().map(row => DatasetsParser.parseTgaTgdDataset(row)).toDS()
  }

  def loadReferentiel(sqlContext : SQLContext) : Dataset[ReferentielGare] = {
    import sqlContext.implicits._

    // Définition du nom de chacune des colonnes car on recoit les fichiers sans headers
    val newNamesRefGares = Seq("CodeGare","IntituleGare","NombrePlateformes","SegmentDRG","UIC","UniteGare","TVS","CodePostal","Commune","DepartementCommune","Departement","Region","AgenceGC","RegionSNCF","NiveauDeService","LongitudeWGS84","LatitudeWGS84","DateFinValiditeGare")

    // Chargement du CSV référentiel
    val refGares = sqlContext.read
      .option("delimiter", ";")
      .option("header", "true")
      .option("charset", "UTF8")
      .format("com.databricks.spark.csv")
      .load(REF_GARES)
      .toDF(newNamesRefGares: _*)
      .as[ReferentielGare]

    // Parsing du CSV a l'intérieur d'un object ReferentielGare
    refGares.toDF().map(DatasetsParser.parseRefGares).toDS()

  }


}
