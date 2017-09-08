package com.sncf.fab.ppiv.pipelineData

import com.sncf.fab.ppiv.utils.AppConf._
import com.sncf.fab.ppiv.utils.Conversion
import org.joda.time.DateTime

/**
  * Created by simoh-labdoui on 11/05/2017.
  */
class TraitementTgd extends SourcePipeline {

  // Pas la même source si l'on est en reprise ou pas
  override def getSource(timeToProcess: DateTime, reprise: Boolean): String = {
    if (reprise == false){
      LANDING_WORK + Conversion.getYearMonthDay(timeToProcess) + "/TGD-" + Conversion
        .getYearMonthDay(timeToProcess) + "_" + Conversion
        .getHourString(timeToProcess) + ".csv"}
    else {REFINERY + Conversion.getYearMonthDay(timeToProcess) + "/TGD-" + Conversion
      .getYearMonthDay(timeToProcess)+ ".csv"}
  }


  override def getOutputRefineryPath(timeToProcess: DateTime) = REFINERY + "ppiv/" +  Conversion.getYearMonthDay(timeToProcess) + "_" + Conversion.getHourString(timeToProcess) + "/output/" + Conversion.getYearMonthDay(timeToProcess) + "_" + Conversion.getHourString(timeToProcess) + ".csv"
  override def getOutputGoldPath(timeToProcess: DateTime) = GOLD + "ppiv/" + Conversion.getYearMonthDay(timeToProcess) + "_" + Conversion.getHourString(timeToProcess) + "/output/" + Conversion.getYearMonthDay(timeToProcess) + "_" + Conversion.getHourString(timeToProcess) + ".csv"
  override def getRejectCycleRefineryPath(timeToProcess: DateTime) = REFINERY + "ppiv/" + Conversion.getYearMonthDay(timeToProcess) + "_" + Conversion.getHourString(timeToProcess) + "/reject_cycle/" +"TGD-"+ Conversion.getYearMonthDay(timeToProcess) + "_" + Conversion.getHourString(timeToProcess) + ".csv"
  override def getRejectCycleGoldPath(timeToProcess: DateTime): String = GOLD + "ppiv/" + Conversion.getYearMonthDay(timeToProcess) + "_" + Conversion.getHourString(timeToProcess) + "/reject_cycle/" +"TGD-"+ Conversion.getYearMonthDay(timeToProcess) + "_" + Conversion.getHourString(timeToProcess) + ".csv"
  override def getRejectFieldRefineryPath(timeToProcess: DateTime): String = REFINERY + "ppiv/" + Conversion.getYearMonthDay(timeToProcess) + "_" + Conversion.getHourString(timeToProcess) + "/reject_field/" +"TGD-"+ Conversion.getYearMonthDay(timeToProcess) + "_" + Conversion.getHourString(timeToProcess) + ".csv"
  override def getRejectFieldGoldPath(timeToProcess: DateTime): String = GOLD + "ppiv/" + Conversion.getYearMonthDay(timeToProcess) + "_" + Conversion.getHourString(timeToProcess) + "/reject_field/" +"TGD-"+ Conversion.getYearMonthDay(timeToProcess) + "_" + Conversion.getHourString(timeToProcess) + ".csv"
  override def Depart(): Boolean = true
  override def Arrive(): Boolean = false
  override def Panneau() : String = "TGD"
}


object TraitementTgd extends TraitementTgd
