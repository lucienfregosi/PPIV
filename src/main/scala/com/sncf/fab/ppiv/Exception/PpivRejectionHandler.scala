package com.sncf.fab.ppiv.Exception

import java.io.FileWriter

import com.codahale.metrics.Gauge
import com.sncf.fab.ppiv.utils.AppConf.EXECUTION_TRACE_FILE
import com.sncf.fab.ppiv.spark.batch.TraitementPPIVDriver.LOGGER
import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.Gauge
import com.sncf.fab.ppiv.Monitoring.GraphiteConf

/**
  * Created by simoh-labdoui on 11/05/2017.
  */

import org.apache.log4j.Logger

object PpivRejectionHandler extends Serializable {


  def handleRejectionFinal(statut: String, dateFichierObier: String, dateExecution: String, currentTgaTgdFile: String, message: String ): Unit = {


    import org.apache.hadoop.mapred.QueueManager

    val metrics = new MetricRegistry
    GraphiteConf.registry.register(MetricRegistry.name(classOf[MetricRegistry], "PPIV", "statut"), new Gauge[Integer]() {
      override def getValue: String = statut })
    // Ecriture d'une ligne dans le fichier final
    write_execution_message(statut,dateFichierObier, dateExecution,currentTgaTgdFile, message)

    // Fin du programme
    LOGGER.warn("ERROR")

  }

  def handleRejection(statut: String, dateFichierObier: String, dateExecution: String, currentTgaTgdFile: String, message: String ): Unit = {
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
