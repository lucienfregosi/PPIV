package com.sncf.fab.ppiv.Exception

import java.io.FileWriter
import com.sncf.fab.ppiv.utils.AppConf.EXECUTION_TRACE_FILE

import com.sncf.fab.ppiv.spark.batch.TraitementPPIVDriver.LOGGER

/**
  * Created by simoh-labdoui on 11/05/2017.
  */

import org.apache.log4j.Logger

object PpivRejectionHandler extends Serializable {


  def handleRejection(statut: String, dateFichierObier: String, dateExecution: String, currentTgaTgdFile: String, message: String ): Unit = {
    // Log de l'erreur
    LOGGER.error("KO Exception renvoye: " + message)

    // Ecriture d'une ligne dans le fichier final
    write_execution_message(statut,dateFichierObier, dateExecution,currentTgaTgdFile, message)

    // Levé d'une exception
    throw new Exception("Exception thrown")

  }

  def write_execution_message(statut: String, dateFichierObier: String, dateExecution: String, currentTgaTgdFile: String, message: String ): Unit ={
    val fw = new FileWriter(EXECUTION_TRACE_FILE, true)
    try {
      fw.write(statut + "," + dateFichierObier + "," + dateExecution + "," + currentTgaTgdFile +  "," + message + "\n")
    }
    finally fw.close()
  }

}
