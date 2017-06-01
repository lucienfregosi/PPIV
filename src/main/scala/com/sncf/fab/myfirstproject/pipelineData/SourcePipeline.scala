package com.sncf.fab.myfirstproject.pipelineData

import com.sncf.fab.myfirstproject.Exception.PpivRejectionHandler
import com.sncf.fab.myfirstproject.business.{QualiteAffichage, TgaTgdParsed}
import com.sncf.fab.myfirstproject.parser.DatasetsParser
import com.sncf.fab.myfirstproject.persistence.{PersistElastic, PersistHdfs, PersistHive, PersistLocal}
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
  sparkSession.sparkContext.getConf.set("es.index.auto.create", "true").set("es.nodes", HOST)

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
    * le traitement principal lancé pour chaque data source
    */

  def start(outputs: Array[String]): Unit = {

    import com.sncf.fab.myfirstproject.parser.DatasetsParser._
    //read data from csv file
    val dsTgaTgd = sparkSession.read.csv(getSource())
    //filter the header to ovoid columns name problem matching
    val header = dsTgaTgd.first()
    val data = dsTgaTgd.filter(_ != header).map(DatasetsParser.parseTgaTgdDataset(_))
    /*Traitement des fichiers*/
    process(data.filter(_ != null), outputs)
  }

  /**
    *
    * @param dsTgaTgd le dataset issu des fichier TGA/TGD (Nettoyé)
    */
  def process(dsTgaTgd: Dataset[TgaTgdParsed], outputs: Array[String]): Unit = {
    try {

      /*
        * convertir les date, nettoyer la data, filtrer la data, sauvegarde dans refinery
        */


      if(outputs.contains("fs"))
        PersistLocal.persisteTgaTgdParsedIntoFs(dsTgaTgd, getOutputRefineryPath())
      if(outputs.contains("hive"))
        PersistHive.persisteTgaTgdParsedHive(dsTgaTgd)
      if(outputs.contains("hdfs"))
        PersistHdfs.persisteTgaTgdParsedIntoHdfs(dsTgaTgd,REFINERY_HDFS)
      if(outputs.contains("es"))
        PersistElastic.persisteTgaTgdParsedIntoEs(dsTgaTgd, TGA_TGD_INDEX)

      val dsQualiteAffichage = clean(dsTgaTgd)


      /*

      sauvegarder le resultat temporairement dans le refinery
       */

      /*
        *Croiser la data avec le refernetiel et sauvegarder dans  un Gold
        */
      PersistElastic.persisteQualiteAffichageIntoEs(dsQualiteAffichage, "")
      PersistLocal.persisteQualiteAffichageIntoFs(dsQualiteAffichage, getOutputGoldPath())
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
    dsTgaTgd.map(row =>
      QualiteAffichage(row.gare, "", Conversion.unixTimestampToDateTime(row.heure).toString, 0,
        row.`type`, "", "", true, true, true, Option(row.retard).nonEmpty)
    )
      //.join()


}



