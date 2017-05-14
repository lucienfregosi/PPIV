package com.sncf.fab.myfirstproject.pipelineData

import java.util.Date
import java.util.Calendar

import com.sncf.fab.myfirstproject.Exception.PpivRejectionHandler
import com.sncf.fab.myfirstproject.business.{QualiteAffichage, TgaTgdParsed}
import com.sncf.fab.myfirstproject.persistence.PersistHive
import com.sncf.fab.myfirstproject.utils.AppConf._
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Created by simoh-labdoui on 11/05/2017.
  */
trait SourcePipeline extends Serializable {


  val sparkSession = SparkSession.builder.
    master(SPARK_MASTER)
    .appName(PPIV)
    .getOrCreate()
  import sparkSession.implicits._

  /**
    *
    * @return le nom de l'application spark visible dans historyserver
    */

  def getAppName(): String = {
    PPIV
  }

  /**
    *
    * @return le chemin de la source de données brute
    */
  def getSource(): String

  /**
    *
    * @return vrai s'il s'agit d'un départ de train, faux s'il s'agit d'un arrivé
    */
  def Depart(): Boolean

  /**
    *
    * @return faux s'il s'agit d'un départ de train, vrai s'il s'agit d'un arrivé
    */
  def Arrive(): Boolean


  /**
    * le traitement pricipal lancé pour chaque data source
    */

  def start(): Unit = {


    //read data from csv file
    val dsTgaTgd = sparkSession.read.option("header", "true").option("inferSchema", "true").csv(getSource()).as[TgaTgdParsed]
    /*Traitement des fichiers*/
    process(dsTgaTgd)
  }

  /**
    *
    * @param dsTgaTgd le dataset issu des fichier TGA/TGD (Nettoyé)
    */
  def process(dsTgaTgd: Dataset[TgaTgdParsed]): Unit = {
    try {
      /*
        * convertir les date, nettoyer la data, filtrer la data, sauvegarde dans refinery
        */

      val date=new java.sql.Date(new Date().getTime)
      val qa=QualiteAffichage("", "",date, 0, "", "", "", true, true, true, true)

      dsTgaTgd.map(dsTgaTgd4=>QualiteAffichage("", "", date, 0, "", "", "", true, true, true, true)).collect().foreach(println)
      val dsQualiteAffichage = clean(dsTgaTgd)
      /*

      sauvegarder le resultat temporairement dans le refinery
       */
      PersistHive.persisteTgaTgdParsedHive(dsTgaTgd)
      /*
        *Croiser la data avec le refernetiel et sauvegarder dans  un Gold
        */
      PersistHive.persisteQualiteAffichageHive(dsQualiteAffichage)
    }
    catch {
      case e: Throwable => {
        PpivRejectionHandler.handleRejection(e.getMessage, PpivRejectionHandler.PROCESSING_ERROR)
        None
      }
    }

  }

  /**
    * Nettoyer la data
    *
    * @param dsTgaTgd issu des fichiers sources TGA/TGD
    */
  def clean(dsTgaTgd: Dataset[TgaTgdParsed]): Dataset[QualiteAffichage] = {
    null
  }


}



