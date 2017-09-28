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
    else {LANDING_WORK_JOURNALIER  + Conversion.getYearMonthDay(timeToProcess) + "/TGD-" + Conversion
      .getYearMonthDay(timeToProcess)+ ".csv"}
  }

  override def getOutputRefineryPath(debutPeriode: DateTime, finPeriode: DateTime, reprise : Boolean) = if (reprise == false ) {REFINERY + "current/output.csv"}
  else {REFINERY + "currentReprise/output.csv"}

  override def getOutputGoldPath(debutPeriode: DateTime, finPeriode: DateTime, reprise : Boolean) = if (reprise == false ) {GOLD + "current/output.csv"}
  else {GOLD + "currentReprise/output.csv"}

  override def getRejectCycleRefineryPath(debutPeriode: DateTime, finPeriode: DateTime, reprise : Boolean) = if (reprise == false ) {REFINERY + "current/reject_cycleTGD.csv"}
  else {REFINERY + "currentReprise/reject_cycleTGD.csv"}

  override def getRejectCycleGoldPath(debutPeriode: DateTime, finPeriode: DateTime, reprise : Boolean) = if (reprise == false ) {GOLD + "current/reject_cycleTGD.csv"}
  else {GOLD + "currentReprise/reject_cycleTGD.csv"}

  override def getRejectFieldRefineryPath(debutPeriode: DateTime, finPeriode: DateTime, reprise : Boolean) = if (reprise == false ) {REFINERY + "current/reject_fieldTGD.csv"}
  else {REFINERY + "currentReprise/reject_fieldTGD.csv"}

  override def getRejectFieldGoldPath(debutPeriode: DateTime, finPeriode: DateTime, reprise : Boolean) = if (reprise == false ) {GOLD + "current/reject_fieldTGD.csv"}
  else {GOLD + "currentReprise/reject_fieldTGD.csv"}

  override def Depart(): Boolean = true
  override def Arrive(): Boolean = false
  override def Panneau() : String = "TGD"
}


object TraitementTgd extends TraitementTgd
