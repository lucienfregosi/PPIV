package com.sncf.fab.myfirstproject.parser

import java.sql.Date
import java.util.Calendar

import com.sncf.fab.myfirstproject.Exception.PpivRejectionHandler
import com.sncf.fab.myfirstproject.business.TgaTgdParsed
import com.sncf.fab.myfirstproject.spark.batch.TraitementPPIVDriver
import com.sncf.fab.myfirstproject.spark.batch.TraitementPPIVDriver.LOGGER
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.collection.mutable

/**
  * Created by Smida Bassem on 16/05/17.
  */
object DatasetsParser {
  var LOGGER = Logger.getLogger(DatasetsParser.getClass)

  def parseTgaTgdDataset(row: Row): TgaTgdParsed = {
    try{
      TgaTgdParsed(row.getString(0), row.getString(1).toLong,
        row.getString(2), row.getString(3), row.getString(4), row.getString(5),
        row.getString(6), row.getString(7), row.getString(8),
        row.getString(9).toLong, row.getString(10), row.getString(11))
    }
    catch {
      case e => {
        PpivRejectionHandler.handleRejection(row.toString(), PpivRejectionHandler.PARSING_ERROR)
        LOGGER.error("Parssing Error for row :"+row.toString()+"\n"+e.getLocalizedMessage)
        null
      }
    }

  }
}

