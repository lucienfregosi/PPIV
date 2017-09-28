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
    else {LANDING_WORK_Journalier  + Conversion.getYearMonthDay(timeToProcess) + "/TGA-" + Conversion
      .getYearMonthDay(timeToProcess)+ ".csv"}
  }


  override def getOutputRefineryPath(debutPeriode: DateTime, finPeriode: DateTime, reprise : Boolean) = if (!reprise) {REFINERY + "current/output.csv"}
  else {REFINERY + "currentReprise/output.csv"}

  override def getOutputGoldPath(debutPeriode: DateTime, finPeriode: DateTime, reprise : Boolean) = if (!reprise) {GOLD + "current/output.csv"}
  else {GOLD + "currentReprise/output.csv"}

  override def getRejectCycleRefineryPath(debutPeriode: DateTime, finPeriode: DateTime, reprise : Boolean) = if (!reprise) {REFINERY + "current/reject_cycleTGA.csv"}
  else {REFINERY + "currentReprise/reject_cycleTGA.csv"}

  override def getRejectCycleGoldPath(debutPeriode: DateTime, finPeriode: DateTime, reprise : Boolean) = if (!reprise) {GOLD + "current/reject_cycleTGA.csv"}
  else {GOLD + "currentReprise/reject_cycleTGA.csv"}

  override def getRejectFieldRefineryPath(debutPeriode: DateTime, finPeriode: DateTime, reprise : Boolean) = if (!reprise) {REFINERY + "current/reject_fieldTGA.csv"}
  else {REFINERY + "currentReprise/reject_fieldTGA.csv"}

  override def getRejectFieldGoldPath(debutPeriode: DateTime, finPeriode: DateTime, reprise : Boolean) = if (!reprise) {GOLD + "current/reject_fieldTGA.csv"}
  else {GOLD + "currentReprise/reject_fieldTGA.csv"}


  override def Depart(): Boolean = false
  override def Arrive(): Boolean = true
  override def Panneau(): String = "TGA"
}

object TraitementTga extends TraitementTga
