package com.sncf.fab.ppiv.pipelineData.libPipeline

import com.sncf.fab.ppiv.business.{ReferentielGare, TgaTgdInput}
import com.sncf.fab.ppiv.parser.DatasetsParser
import com.sncf.fab.ppiv.utils.AppConf.REF_GARES
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{Dataset, SQLContext}
import java.nio.file.{Files, Paths}

import com.sncf.fab.ppiv.Exception.PpivRejectionHandler
import com.sncf.fab.ppiv.spark.batch.TraitementPPIVDriver
import com.sncf.fab.ppiv.utils.Conversion
import org.apache.spark.SparkContext

/**
  * Created by ELFI03951 on 12/07/2017.
  */
object LoadData {
  def loadTgaTgd(sqlContext : SQLContext, path: String): Dataset[TgaTgdInput] = {
    import sqlContext.implicits._

    // Définition du nom de chacune des colonnes car on recoit les fichiers sans headers
    val newNamesTgaTgd = Seq("gare","maj","train","ordes","num","type","picto","attribut_voie","voie","heure","etat","retard","null")

    // Test si le fichier existe
    if(!checkIfFileExist(sqlContext.sparkContext,path )) {
      PpivRejectionHandler.handleRejection("KO",Conversion.getHourDebutPlageHoraire(TraitementPPIVDriver.startTimePipeline),TraitementPPIVDriver.startTimePipeline.toString(),path, "Le fichier n'existe pas")
    }

    // Lecture du CSV avec les bons noms de champs
    val dfTgaTgd = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("delimiter", ";")
      .load(path).toDF(newNamesTgaTgd: _*)
      .withColumn("maj", 'maj.cast(LongType))
      .withColumn("heure", 'heure.cast(LongType))
      .filter($"maj".isNotNull)
      .filter($"heure".isNotNull)
      .distinct()
      .as[TgaTgdInput]

    dfTgaTgd

    // Parsing du CSV a l'intérieur d'un object TgaTgaInput, conversion en dataset
    //dsTgaTgd.map(row => DatasetsParser.parseTgaTgdDataset(row)).toDS()
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
      .distinct()
      .as[ReferentielGare]

    // Parsing du CSV a l'intérieur d'un object ReferentielGare, conversion en dataset
    refGares.toDF().map(DatasetsParser.parseRefGares).toDS()

  }

  // Retourne true si le fichier existe
  def checkIfFileExist(sc: SparkContext, path: String): Boolean ={
    val conf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)
    fs.exists(new org.apache.hadoop.fs.Path(path))
  }


}
