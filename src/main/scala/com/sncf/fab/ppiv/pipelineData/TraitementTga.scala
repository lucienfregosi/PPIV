package com.sncf.fab.ppiv.pipelineData

import com.sncf.fab.ppiv.utils.AppConf._
import com.sncf.fab.ppiv.utils.Conversion
import org.joda.time.DateTime

/**
  * Created by simoh-labdoui on 11/05/2017.
  */
class TraitementTga extends SourcePipeline {


  override def getSource(timeToProcess: DateTime) = LANDING_WORK  + Conversion.getYearMonthDay(timeToProcess) + "/TGA-" + Conversion.getYearMonthDay(timeToProcess) + "_" + Conversion.getHourString(timeToProcess) + ".csv"

  override def getOutputRefineryPath(timeToProcess: DateTime) = REFINERY + "ppiv/" + Conversion.getYearMonthDay(timeToProcess) + "_" + Conversion.getHourString(timeToProcess) + "/output/" + Conversion.getYearMonthDay(timeToProcess) + "_" + Conversion.getHourString(timeToProcess) + ".csv"
  override def getOutputGoldPath(timeToProcess: DateTime) = GOLD + "ppiv/" + Conversion.getYearMonthDay(timeToProcess) + "_" + Conversion.getHourString(timeToProcess) + "/output/" + Conversion.getYearMonthDay(timeToProcess) + "_" + Conversion.getHourString(timeToProcess) + ".csv"
  override def getRejectCycleRefineryPath(timeToProcess: DateTime) = REFINERY + "ppiv/" + Conversion.getYearMonthDay(timeToProcess) + "_" + Conversion.getHourString(timeToProcess) + "/reject_cycle/" +"TGA-"+ Conversion.getYearMonthDay(timeToProcess) + "_" + Conversion.getHourString(timeToProcess) + ".csv"
  override def getRejectCycleGoldPath(timeToProcess: DateTime): String = GOLD + "ppiv/" + Conversion.getYearMonthDay(timeToProcess) + "_" + Conversion.getHourString(timeToProcess) + "/reject_cycle/" +"TGA-"+ Conversion.getYearMonthDay(timeToProcess) + "_" + Conversion.getHourString(timeToProcess) + ".csv"
  override def getRejectFieldRefineryPath(timeToProcess: DateTime): String = REFINERY + "ppiv/" + Conversion.getYearMonthDay(timeToProcess) + "_" + Conversion.getHourString(timeToProcess) + "/reject_field/" +"TGA-"+ Conversion.getYearMonthDay(timeToProcess) + "_" + Conversion.getHourString(timeToProcess) + ".csv"
  override def getRejectFieldGoldPath(timeToProcess: DateTime): String = GOLD + "ppiv/" + Conversion.getYearMonthDay(timeToProcess) + "_" + Conversion.getHourString(timeToProcess) + "/reject_field/" +"TGA-"+ Conversion.getYearMonthDay(timeToProcess) + "_" + Conversion.getHourString(timeToProcess) + ".csv"
  override def Depart(): Boolean = false
  override def Arrive(): Boolean = true
  override def Panneau(): String = "TGA"
}

object TraitementTga extends TraitementTga
