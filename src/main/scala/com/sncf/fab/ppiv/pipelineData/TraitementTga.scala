package com.sncf.fab.ppiv.pipelineData

import com.sncf.fab.ppiv.utils.AppConf._
import com.sncf.fab.ppiv.utils.Conversion
import org.joda.time.DateTime

/**
  * Created by simoh-labdoui on 11/05/2017.
  */
class TraitementTga extends SourcePipeline {


  override def getSource(timeToProcess: DateTime) = LANDING_WORK  + Conversion.getYearMonthDay(timeToProcess) + "/TGA-" + Conversion.getYearMonthDay(timeToProcess) + "_" + Conversion.getHourDebutPlageHoraire(timeToProcess) + ".csv"

  override def getOutputGoldPath(timeToProcess: DateTime) = GOLD + Conversion.getYearMonthDay(timeToProcess) + "_" + Conversion.getHourDebutPlageHoraire(timeToProcess) + ".csv"

  override def getOutputRefineryPath(timeToProcess: DateTime) = REFINERY + Conversion.getYearMonthDay(timeToProcess) + "_" + Conversion.getHourDebutPlageHoraire(timeToProcess) + ".csv"

  override def Depart(): Boolean = false

  override def Arrive(): Boolean = true

  override def Panneau() : String = "TGA"
}

object TraitementTga extends TraitementTga
