package com.sncf.fab.myfirstproject.pipelineData

import com.sncf.fab.myfirstproject.Exception.PpivRejectionHandler
import com.sncf.fab.myfirstproject.business.{QualiteAffichage, RefGaresParsed, TgaTgdParsed}
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
    * le traitement pricipal lancé pour chaque data source
    */

  def start(outputs: Array[String]): Unit = {

    import com.sncf.fab.myfirstproject.parser.DatasetsParser._
    //read data from csv file
    val dsTgaTgd = sparkSession.read.csv(getSource())
    //filter the header to ovoid columns name problem matching
    val headerTgaTgd = dsTgaTgd.first()
    val dataTgaTgd = dsTgaTgd.filter(_ != headerTgaTgd).map(DatasetsParser.parseTgaTgdDataset)
    val refGares = sparkSession.read.option("delimiter", ";").csv(LANDING_WORK + REF_GARES)
    val headerRefGares = refGares.first()
    val dataRefGares = refGares.filter(_ != headerRefGares).map(DatasetsParser.parseRefGares)
    dataRefGares.take(1).foreach(println)
    /*Traitement des fichiers*/
    process(dataTgaTgd.filter(_ != null), dataRefGares, outputs)
  }

  /**
    *
    * @param dsTgaTgd le dataset issu des fichier TGA/TGD (Nettoyé)
    */
  def process(dsTgaTgd: Dataset[TgaTgdParsed], refGares: Dataset[RefGaresParsed], outputs: Array[String]): Unit = {
    try {
      val qualiteAffichage = joinData(dsTgaTgd, refGares)

      if (outputs.contains("fs"))
        PersistLocal.persisteTgaTgdParsedIntoFs(dsTgaTgd, getOutputRefineryPath())
      if (outputs.contains("hive"))
        PersistHive.persisteTgaTgdParsedHive(dsTgaTgd)
      if (outputs.contains("hdfs"))
        PersistHdfs.persisteTgaTgdParsedIntoHdfs(dsTgaTgd, REFINERY_HDFS)
      if (outputs.contains("es"))
        PersistElastic.persisteQualiteAffichageIntoEs(qualiteAffichage, QUALITE_INDEX)

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
    * Jointure avec RefGares
    *
    * @param dsTgaTgd issu des fichiers sources TGA/TGD
    * @param refGares issu des fichiers sources refGares
    */
  def joinData(dsTgaTgd: Dataset[TgaTgdParsed], refGares: Dataset[RefGaresParsed]): Dataset[QualiteAffichage] = {
    val finals = dsTgaTgd.join(refGares, dsTgaTgd("gare") === refGares("tvs"))
    finals.map(row => QualiteAffichage(row.getString(0), row.getString(15),
      Conversion.unixTimestampToDateTime(row.getLong(9)).toString, row.getString(13),
      row.getString(5), "", "", true, true, Option(row.getString(11)).nonEmpty, Option(row.getString(8)).nonEmpty && row.getString(8) != "0",
      row.getString(16), row.getString(17), row.getString(18)
    ))
  }


}



