package com.sncf.fab.ppiv.spark.batch

import com.sncf.fab.ppiv.Exception.PpivRejectionHandler
import com.sncf.fab.ppiv.persistence.{PersistElastic, PersistHdfs, PersistHive, PersistLocal}
import com.sncf.fab.ppiv.pipelineData.{SourcePipeline, TraitementTga, TraitementTgd}
import org.apache.log4j.Logger
import com.sncf.fab.ppiv.utils.AppConf._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
//  * Created by simoh-labdoui on 11/05/2017.
//  */
object TraitementPPIVDriver extends Serializable {
  var LOGGER = Logger.getLogger(TraitementPPIVDriver.getClass)
  LOGGER.info("Traitement d'affichage des trains")

  def main(args: Array[String]): Unit = {
    if (args.length == 0){
      LOGGER.error("Wrong number of parameters")
      System.exit(1)
    }
    else {

      // Définition du Spark Context et SQL Context
      @transient val sparkConf = getSparkConf()
      @transient val sc = new SparkContext(sparkConf)
      @transient val sqlContext = new SQLContext(sc)


      LOGGER.info("Traitement d'affichage des trains TGAAAAAAAAAAAAAAAAAAAAAA")
      val dataTga = TraitementTga.start(args, sc, sqlContext)


      LOGGER.info("Traitement d'affichage des trains TGD")
      val dataTgd = TraitementTgd.start(args, sc, sqlContext)


      // 11) Fusion des résultats de TGA et TGD
      val dataTgaAndTga = dataTga.union(dataTgd)

      // 12) Sauvegarde la ou nous l'a demandé
      try {
        if (args.contains("fs"))
          PersistLocal.persisteQualiteAffichageIntoFs(dataTgaAndTga, TraitementTga.getOutputRefineryPath())
        if (args.contains("hive"))
          PersistHive.persisteQualiteAffichageHive(dataTgaAndTga)
        if (args.contains("hdfs"))
          PersistHdfs.persisteQualiteAffichageIntoHdfs(dataTgaAndTga, TraitementTga.getOutputRefineryPath())
        if (args.contains("es"))
          PersistElastic.persisteQualiteAffichageIntoEs(dataTgaAndTga, QUALITE_INDEX)
      }
      catch {
        case e: Throwable => {
          e.printStackTrace()
          PpivRejectionHandler.handleRejection(e.getMessage, PpivRejectionHandler.PROCESSING_ERROR)
          None
        }
      }
    }
  }

  def getSparkConf() : SparkConf = {
    new SparkConf()
      .setAppName(PPIV)
      .setMaster("yarn")
      .set("es.nodes", HOST)
      .set("es.port", "9201")
      .set("es.index.auto.create", "true")
  }


}

