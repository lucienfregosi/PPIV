package com.sncf.fab.ppiv.pipelineData

import com.sncf.fab.ppiv.utils.AppConf._
import com.sncf.fab.ppiv.utils.Conversion
import org.joda.time.DateTime

/**
  * Created by simoh-labdoui on 11/05/2017.
  */
class TraitementTga extends SourcePipeline {


  // Pas la même source si l'on est en reprise ou pas
  override def getSource(timeToProcess: DateTime, reprise: Boolean): String = {
    if (reprise == false){
      LANDING_WORK + Conversion.getYearMonthDay(timeToProcess) + "/TGA-" + Conversion
        .getYearMonthDay(timeToProcess) + "_" + Conversion
        .getHourString(timeToProcess) + ".csv"}
    else {REFINERY + Conversion.getYearMonthDay(timeToProcess) + "/TGA-" + Conversion
      .getYearMonthDay(timeToProcess)+ ".csv"}
  }


  override def getOutputRefineryPath(debutPeriode: DateTime, finPeriode: DateTime, reprise : Boolean) = if (reprise == false ) {REFINERY + "ppiv/" + Conversion.getYearMonthDay(debutPeriode) + "_" + Conversion.getHourString(debutPeriode) + "/output/" + Conversion.getYearMonthDay(debutPeriode) + "_" + Conversion.getHourString(debutPeriode)+ ".csv"}
  else {REFINERY + "ppiv/" + Conversion.getYearMonthDay(debutPeriode) + "_" + Conversion.getHourString(debutPeriode)+ "_" + Conversion.getHourString(finPeriode) + "/output/" + Conversion.getYearMonthDay(debutPeriode) + "_" + Conversion.getHourString(debutPeriode)+ "_" + Conversion.getHourString(finPeriode)+ ".csv"}
  override def getOutputGoldPath(debutPeriode: DateTime, finPeriode: DateTime, reprise : Boolean) = if (reprise == false ) {GOLD + "ppiv/" + Conversion.getYearMonthDay(debutPeriode) + "_" + Conversion.getHourString(debutPeriode) + "/output/" + Conversion.getYearMonthDay(debutPeriode) + "_" + Conversion.getHourString(debutPeriode) + ".csv"}
  else {GOLD + "ppiv/" + Conversion.getYearMonthDay(debutPeriode) + "_" + Conversion.getHourString(debutPeriode) + "_" + Conversion.getHourString(finPeriode)+ "/output/" + Conversion.getYearMonthDay(debutPeriode) + "_" + Conversion.getHourString(debutPeriode)+ "_" + Conversion.getHourString(finPeriode)+".csv"}

  override def getRejectCycleRefineryPath(debutPeriode: DateTime, finPeriode: DateTime, reprise : Boolean) = if ( reprise == false) {REFINERY + "ppiv/" + Conversion.getYearMonthDay(debutPeriode) + "_" + Conversion.getHourString(debutPeriode) + "/reject_cycle/" +"TGA-"+ Conversion.getYearMonthDay(debutPeriode) + "_" + Conversion.getHourString(debutPeriode) + ".csv"}
  else {REFINERY + "ppiv/" + Conversion.getYearMonthDay(debutPeriode) + "_" + Conversion.getHourString(debutPeriode)+ "_" + Conversion.getHourString(finPeriode) + "/reject_cycle/" +"TGA-"+ Conversion.getYearMonthDay(debutPeriode) + "_" + Conversion.getHourString(debutPeriode)+ "_" + Conversion.getHourString(finPeriode)+".csv"}
  override def getRejectCycleGoldPath(debutPeriode: DateTime, finPeriode: DateTime, reprise : Boolean): String = if ( reprise == false) {GOLD + "ppiv/" + Conversion.getYearMonthDay(debutPeriode) + "_" + Conversion.getHourString(debutPeriode) + "/reject_cycle/" +"TGA-"+ Conversion.getYearMonthDay(debutPeriode) + "_" + Conversion.getHourString(debutPeriode) + ".csv"}
  else {GOLD + "ppiv/" + Conversion.getYearMonthDay(debutPeriode) + "_" + Conversion.getHourString(debutPeriode) + "_" + Conversion.getHourString(finPeriode)+ "/reject_cycle/" +"TGA-"+ Conversion.getYearMonthDay(debutPeriode)+ "_" + Conversion.getHourString(debutPeriode) + "_" + Conversion.getHourString(finPeriode)+ ".csv"}

  override def getRejectFieldRefineryPath(debutPeriode: DateTime, finPeriode: DateTime, reprise : Boolean): String = if (reprise == false) { REFINERY + "ppiv/" + Conversion.getYearMonthDay(debutPeriode) + "_" + Conversion.getHourString(debutPeriode) + "/reject_field/" +"TGA-"+ Conversion.getYearMonthDay(debutPeriode) + "_" + Conversion.getHourString(debutPeriode) + ".csv"}
  else { REFINERY + "ppiv/" + Conversion.getYearMonthDay(debutPeriode) + "_" + Conversion.getHourString(debutPeriode) + "_" + Conversion.getHourString(finPeriode)+ "/reject_field/" +"TGA-"+ Conversion.getYearMonthDay(debutPeriode) + "_" + Conversion.getHourString(debutPeriode) + "_" + Conversion.getHourString(finPeriode)+ ".csv"}
  override def getRejectFieldGoldPath(debutPeriode: DateTime, finPeriode: DateTime, reprise : Boolean): String =  if (reprise == false) {GOLD + "ppiv/" + Conversion.getYearMonthDay(debutPeriode) + "_" + Conversion.getHourString(debutPeriode) + "/reject_field/" +"TGA-"+ Conversion.getYearMonthDay(debutPeriode) + "_" + Conversion.getHourString(debutPeriode) + ".csv"}
  else {GOLD + "ppiv/" + Conversion.getYearMonthDay(debutPeriode) + "_" + Conversion.getHourString(debutPeriode)+ "_" + Conversion.getHourString(finPeriode) + "/reject_field/" +"TGA-"+ Conversion.getYearMonthDay(debutPeriode) + "_" + Conversion.getHourString(debutPeriode)+  "_" + Conversion.getHourString(finPeriode)+".csv"}

  override def Depart(): Boolean = false
  override def Arrive(): Boolean = true
  override def Panneau(): String = "TGA"
}

object TraitementTga extends TraitementTga
