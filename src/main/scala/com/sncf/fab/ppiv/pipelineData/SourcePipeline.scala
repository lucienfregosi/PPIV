package com.sncf.fab.ppiv.pipelineData

import com.sncf.fab.ppiv.Exception.PpivRejectionHandler
import com.sncf.fab.ppiv.business.{QualiteAffichage, RefGaresParsed, TgaTgdParsed}
import com.sncf.fab.ppiv.parser.DatasetsParser
import com.sncf.fab.ppiv.persistence.{PersistElastic, PersistHdfs, PersistHive, PersistLocal}
import com.sncf.fab.ppiv.utils.AppConf._
import org.apache.spark.SparkConf
import com.sncf.fab.ppiv.utils.Conversion
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext}
import org.apache.spark.SparkContext

/**
  * Created by simoh-labdoui on 11/05/2017.
  */
trait SourcePipeline extends Serializable {


  val sparkConf = getSparkConf()
  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)
  def getSparkConf() : SparkConf = {
    new SparkConf()
      .setAppName(PPIV)
      .setMaster(SPARK_MASTER)
      .set("es.nodes", HOST)
      .set("es.port", "9201")
      .set("es.index.auto.create", "true")

  }




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
    import sqlContext.implicits._
    //read data from csv file
    val dsTgaTgd = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(getSource()).as[TgaTgdParsed];

    val refGares = sqlContext.read.
      option("delimiter", ";")
      .format("com.databricks.spark.csv")
      .load(LANDING_WORK + REF_GARES).as[RefGaresParsed]



    //filter the header to ovoid columns name problem matching
    val headerTgaTgd = dsTgaTgd.first()
    val dataTgaTgd = dsTgaTgd.filter(_ != headerTgaTgd).toDF().map(row => DatasetsParser.parseTgaTgdDataset(row)).toDS()

    val headerRefGares = refGares.first()
    val dataRefGares = refGares.filter(_ != headerRefGares).toDF().map(DatasetsParser.parseRefGares).toDS()
    /*Traitement des fichiers*/
    process(dataTgaTgd, dataRefGares, outputs)
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
    import sqlContext.implicits._
    val finals = dsTgaTgd.joinWith(refGares, dsTgaTgd.toDF().col("gare") === refGares.toDF().col("tvs"))
    finals.toDF().map(row => QualiteAffichage(row.getString(0), row.getString(15),
      Conversion.unixTimestampToDateTime(row.getLong(9)).toString, row.getString(13),
      row.getString(5), "", "", true, true, Option(row.getString(11)).nonEmpty, Option(row.getString(8)).nonEmpty && row.getString(8) != "0",
      row.getString(16), row.getString(17), row.getString(18)
    ))
    finals.toDS().as[QualiteAffichage]
  }


}



