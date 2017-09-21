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
    else {LANDING_WORK_2  + Conversion.getYearMonthDay(timeToProcess) + "/TGA-" + Conversion
      .getYearMonthDay(timeToProcess)+ ".csv"}
  }

  override def getOutputRefineryPath(debutPeriode: DateTime, finPeriode: DateTime, reprise : Boolean) = if (reprise == false ) {REFINERY + "current/output.csv"}
  else {REFINERY + "currentReprise/output.csv"}

  override def getOutputGoldPath(debutPeriode: DateTime, finPeriode: DateTime, reprise : Boolean) = if (reprise == false ) {GOLD + "current/output.csv"}
  else {GOLD + "currentReprise/output.csv"}

  override def getRejectCycleRefineryPath(debutPeriode: DateTime, finPeriode: DateTime, reprise : Boolean) = if (reprise == false ) {REFINERY + "current/reject_cycle.csv"}
  else {REFINERY + "currentReprise/reject_cycle.csv"}

  override def getRejectCycleGoldPath(debutPeriode: DateTime, finPeriode: DateTime, reprise : Boolean) = if (reprise == false ) {GOLD + "current/reject_cycle.csv"}
  else {GOLD + "currentReprise/reject_cycle.csv"}

  override def getRejectFieldRefineryPath(debutPeriode: DateTime, finPeriode: DateTime, reprise : Boolean) = if (reprise == false ) {REFINERY + "current/reject_field.csv"}
  else {REFINERY + "currentReprise/reject_field.csv"}

  override def getRejectFieldGoldPath(debutPeriode: DateTime, finPeriode: DateTime, reprise : Boolean) = if (reprise == false ) {GOLD + "current/reject_field.csv"}
  else {GOLD + "currentReprise/reject_field.csv"}

  override def Depart(): Boolean = true
  override def Arrive(): Boolean = false
  override def Panneau() : String = "TGD"
}


object TraitementTgd extends TraitementTgd
