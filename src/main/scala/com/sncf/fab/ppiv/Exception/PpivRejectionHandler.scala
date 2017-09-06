package com.sncf.fab.ppiv.Exception

import com.sncf.fab.ppiv.spark.batch.TraitementPPIVDriver.LOGGER

/**
  * Created by simoh-labdoui on 11/05/2017.
  */

import org.apache.log4j.Logger

object PpivRejectionHandler extends Serializable {


  def handleRejection(message: String): Unit = {
    // Log de l'erreur
    LOGGER.error("KO Exception renvoye: " + message)

    // Ecriture d'une ligne dans le fichier final

    // Exit du programme

    System.exit(0)

  }
}
