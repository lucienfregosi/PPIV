package com.sncf.fab.ppiv.pipelineData

import com.sncf.fab.ppiv.utils.AppConf._
import com.sncf.fab.ppiv.utils.Conversion
import org.joda.time.DateTime

/**
  * Created by simoh-labdoui on 11/05/2017.
  */
class TraitementTgd extends SourcePipeline {

  override def getSource(timeToProcess: DateTime, reprise : Boolean) = {

    if (!reprise){
      LANDING_WORK + Conversion.getYearMonthDay(timeToProcess) + "/TGA-" + Conversion
        .getYearMonthDay(timeToProcess) + "_" + Conversion
        .getHourDebutPlageHoraire(timeToProcess) + ".csv"}
    else {REFINERY + Conversion.getYearMonthDay(timeToProcess) + "/TGA-" + Conversion
      .getYearMonthDay(timeToProcess)}
  }
  override def getOutputGoldPath(timeToProcess: DateTime) = GOLD + Conversion.getYearMonthDay(timeToProcess) + "_" + Conversion.getHourDebutPlageHoraire(timeToProcess) + ".csv"

  override def getOutputRefineryPath(timeToProcess: DateTime) = REFINERY + Conversion.getYearMonthDay(timeToProcess) + "_" + Conversion.getHourDebutPlageHoraire(timeToProcess) + ".csv"

  override def Depart(): Boolean = true

  override def Arrive(): Boolean = false

  override def Panneau() : String = "TGD"
}


object TraitementTgd extends TraitementTgd
