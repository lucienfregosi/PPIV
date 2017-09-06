package com.sncf.fab.ppiv.Exception

import java.io.FileWriter
import com.sncf.fab.ppiv.utils.AppConf.EXECUTION_TRACE_FILE

import com.sncf.fab.ppiv.spark.batch.TraitementPPIVDriver.LOGGER

/**
  * Created by simoh-labdoui on 11/05/2017.
  */

import org.apache.log4j.Logger

object PpivRejectionHandler extends Serializable {


  def handleRejection(statut: String, dateExecution: String, currentTgaTgdFile: String, message: String ): Unit = {
    // Log de l'erreur
    LOGGER.error("KO Exception renvoye: " + message)

    // Ecriture d'une ligne dans le fichier final
    write_execution_message(statut, dateExecution,currentTgaTgdFile, message)
    // Exit du programme
    System.exit(0)

  }

  def write_execution_message(statut: String, dateExecution: String, currentTgaTgdFile: String, message: String ): Unit ={
    val fw = new FileWriter(EXECUTION_TRACE_FILE, true)
    try {
      fw.write("statut,dateExecution,fichierImplique,message\n")
      fw.write(statut + "," + dateExecution + "," + currentTgaTgdFile +  "," + message + "\n")
    }
    finally fw.close()
  }

}
