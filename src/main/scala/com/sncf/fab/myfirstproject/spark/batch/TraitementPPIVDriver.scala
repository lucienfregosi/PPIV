package com.sncf.fab.myfirstproject.spark.batch

import com.sncf.fab.myfirstproject.pipelineData.{SourcePipeline, TraitementTga, TraitementTgd}
import org.apache.log4j.Logger

/**
  * Created by simoh-labdoui on 11/05/2017.
  */
object TraitementPPIVDriver extends Serializable {
  var LOGGER = Logger.getLogger(TraitementPPIVDriver.getClass)
  LOGGER.info("Traitement d'affichage des trains")

  def main(args: Array[String]): Unit = {
    LOGGER.info("Traitement d'affichage des trains TGA")
    TraitementTga.start()
    LOGGER.info("Traitement d'affichage des trains TGD")
//    TraitementTgd.start()
  }


}

