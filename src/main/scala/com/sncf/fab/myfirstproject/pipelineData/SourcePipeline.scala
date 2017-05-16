package com.sncf.fab.myfirstproject.pipelineData

import java.sql.Timestamp
import java.util.Date
import java.util.Calendar

import com.sncf.fab.myfirstproject.Exception.PpivRejectionHandler
import com.sncf.fab.myfirstproject.business.{QualiteAffichage, TgaTgdParsed}
import com.sncf.fab.myfirstproject.persistence.{PersistHive, PersistLocal}
import com.sncf.fab.myfirstproject.utils.AppConf._
import com.sncf.fab.myfirstproject.utils.Conversion
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
    * @return le chemin de l'output qualité
    */
  def getOutputGoldPath(): String

  /**
    *
    * @return the path used to store the cleaned TgaTgaPased
    */
  def getOutputRefineryPath(): String



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
    val dsTgaTgd = sparkSession.read.csv(getSource())
    //filter the header to ovoid columns name problem matching
    val header = dsTgaTgd.first()
    val data = dsTgaTgd.filter(_ != header).map(row => TgaTgdParsed(row.getString(0), row.getString(1).toLong,
      row.getString(2), row.getString(3), row.getString(4), row.getString(5),
      row.getString(6), row.getString(7), row.getString(8),
      row.getString(9).toLong, row.getString(10), row.getString(11)))
    /*Traitement des fichiers*/
    process(data)
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
      PersistLocal.persisteTgaTgdParsedIntoFs(dsTgaTgd,getOutputRefineryPath())

      val dsQualiteAffichage = clean(dsTgaTgd)

      /*

      sauvegarder le resultat temporairement dans le refinery
       */

      PersistHive.persisteTgaTgdParsedHive(dsTgaTgd)
      /*
        *Croiser la data avec le refernetiel et sauvegarder dans  un Gold
        */
      PersistLocal.persisteQualiteAffichageIntoFs(dsQualiteAffichage,getOutputGoldPath())
    }
    catch {
      case e: Throwable => {
        e.printStackTrace()
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
  def clean(dsTgaTgd: Dataset[TgaTgdParsed]): Dataset[QualiteAffichage] =
    dsTgaTgd.map(row => QualiteAffichage(row.gare, "", Conversion.unixTimestampToDateTime(row.heure).toString, 0, row.`type`, "", "", true, true, true, Option(row.retard).nonEmpty)
    )


}



