package com.sncf.fab.ppiv.pipelineData.libPipeline

import java.io.{PrintWriter, StringWriter}

import com.sncf.fab.ppiv.business.{ReferentielGare, TgaTgdInput}
import com.sncf.fab.ppiv.parser.DatasetsParser
import com.sncf.fab.ppiv.utils.AppConf.REF_GARES
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{Dataset, SQLContext}
import java.nio.file.{Files, Paths}

import com.sncf.fab.ppiv.Exception.PpivRejectionHandler
import com.sncf.fab.ppiv.spark.batch.TraitementPPIVDriver
import org.apache.spark.SparkContext
import org.joda.time.DateTime

/**
  * Created by ELFI03951 on 12/07/2017.
  */
object LoadData {
  def loadTgaTgd(sqlContext : SQLContext, path: String, debutPeriode: DateTime, reprise_flag : Boolean): Dataset[TgaTgdInput] = {
    import sqlContext.implicits._


    // Définition du nom de chacune des colonnes car on recoit les fichiers sans headers
    val newNamesTgaTgd = if (reprise_flag == false) {
      Seq("gare", "maj", "train", "ordes", "num", "type", "picto", "attribut_voie", "voie", "heure", "etat", "retard", "null")
    }
    else {
      Seq("gare", "maj", "train", "ordes", "num", "type", "picto", "attribut_voie", "voie", "heure", "etat", "retard")
    }


    // Test si le fichier existe
    if(!checkIfFileExist(sqlContext.sparkContext,path )) {
      PpivRejectionHandler.handleRejection("KO",debutPeriode.toString, TraitementPPIVDriver.startTimePipeline.toString(),path, "Le fichier " + path + " n'existe pas")
    }

    try{
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
    }
    catch {
      case e: Throwable => {
        // Retour d'une valeur par défaut
        PpivRejectionHandler.handleRejection("KO",debutPeriode.toString(), TraitementPPIVDriver.startTimePipeline.toString(),path, "Impossible de parser le fichier " + path + " " + e)
        null
      }
    }



  }

  def loadReferentiel(sqlContext : SQLContext, debutPeriode: DateTime) : Dataset[ReferentielGare] = {
    import sqlContext.implicits._

    // Définition du nom de chacune des colonnes car on recoit les fichiers sans headers
    val newNamesRefGares = Seq("CodeGare","IntituleGare","NombrePlateformes","SegmentDRG","UIC","UniteGare","TVS","CodePostal","Commune","DepartementCommune","Departement","Region","AgenceGC","RegionSNCF","NiveauDeService","LongitudeWGS84","LatitudeWGS84","DateFinValiditeGare")

    // Test si le fichier existe
    if(!checkIfFileExist(sqlContext.sparkContext,REF_GARES )) {
      PpivRejectionHandler.handleRejection("KO",debutPeriode.toString, TraitementPPIVDriver.startTimePipeline.toString(),REF_GARES, "Le référentiel  " + REF_GARES + " n'existe pas")
    }

    try{
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
    catch {
      case e: Throwable => {
        // Retour d'une valeur par défaut
        PpivRejectionHandler.handleRejection("KO",debutPeriode.toString(), TraitementPPIVDriver.startTimePipeline.toString(),REF_GARES, "Impossible de parser le référentiel: " + REF_GARES + " " + e)
        null
      }
    }


  }

  // Retourne true si le fichier existe
  def checkIfFileExist(sc: SparkContext, path: String): Boolean ={
    val conf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)
    fs.exists(new org.apache.hadoop.fs.Path(path))
  }


}
