package com.sncf.fab.ppiv.Exception

import java.io.FileWriter

import com.codahale.metrics.Gauge
import com.sncf.fab.ppiv.utils.AppConf.EXECUTION_TRACE_FILE
import com.sncf.fab.ppiv.spark.batch.TraitementPPIVDriver.LOGGER
import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.Gauge
import com.sncf.fab.ppiv.Monitoring.GraphiteConf

/**
  * Created by Kaoula GHRIBI .
  */

import org.apache.log4j.Logger

object PpivRejectionHandler extends Serializable {


  def manageGraphite(statut: Integer): Unit = {
    // !! 1 signifie erreur
    // et 0 signifie succès

    GraphiteConf.registry.register(MetricRegistry.name(classOf[MetricRegistry], "PPIV", "statut"), new Gauge[Integer]() {
      override def getValue : Integer = statut })
    GraphiteConf.reporter.report()
    Thread.sleep(5*1000)
  }

  def handleRejectionFinProgramme(statut: String, dateFichierObier: String, dateExecution: String, currentTgaTgdFile: String, message: String ): Unit = {

    // Ecriture des métriques KO dans Graphite
    manageGraphite(1)

     // Ecriture d'une ligne dans le fichier final
    write_execution_message(statut,dateFichierObier, dateExecution,currentTgaTgdFile, message)

    // Fin du programme
    LOGGER.warn("ERROR")

  }

  def handleRejectionError(statut: String, dateFichierObier: String, dateExecution: String, currentTgaTgdFile: String, message: String ): Unit = {
    // Log de l'erreur
    val log = "KO Exception renvoye: " + message
    LOGGER.error(log)

    // Levé d'une exception
    throw new Exception(log)
  }

  def write_execution_message(statut: String, dateFichierObier: String, dateExecution: String, currentTgaTgdFile: String, message: String ): Unit ={

    // Ecriture sur le fichier de sortie résumant l'exécution du programme
    val fw = new FileWriter(EXECUTION_TRACE_FILE, true)
    try {
      fw.write(statut + "," + dateFichierObier + "," + dateExecution + "," + currentTgaTgdFile +  "," + message + "\n")
    }
    finally fw.close()

    // Publication des métriques dans graphite

  }

}
