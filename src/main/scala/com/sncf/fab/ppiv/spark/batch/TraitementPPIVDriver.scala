package com.sncf.fab.ppiv.spark.batch

import com.sncf.fab.ppiv.pipelineData.{SourcePipeline, TraitementTga, TraitementTgd}
import org.apache.log4j.Logger
import com.sncf.fab.ppiv.utils.AppConf._

/**
//  * Created by simoh-labdoui on 11/05/2017.
//  */
object TraitementPPIVDriver extends Serializable {
  var LOGGER = Logger.getLogger(TraitementPPIVDriver.getClass)
  LOGGER.info("Traitement d'affichage des trains")

  def main(args: Array[String]): Unit = {
    if (args.length == 0){
      LOGGER.error("Wrong number of parameters")
      println(GOLD)
      System.exit(1)
    }
    else {
      LOGGER.info("Traitement d'affichage des trains TGA")
      TraitementTga.start(args)
      LOGGER.info("Traitement d'affichage des trains TGD")
    //  TraitementTgd.start(args)
    }
  }


}

