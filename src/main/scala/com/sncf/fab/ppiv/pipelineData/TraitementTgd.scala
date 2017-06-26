package com.sncf.fab.ppiv.pipelineData

import com.sncf.fab.ppiv.utils.AppConf._
import com.sncf.fab.ppiv.utils.Conversion
import org.joda.time.DateTime

/**
  * Created by simoh-labdoui on 11/05/2017.
  */
class TraitementTgd extends SourcePipeline {

  override def getSource() = LANDING_WORK  + Conversion.getYearMonthDay(Conversion.nowToDateTime()) + "/TGD-" + Conversion.getYearMonthDay(Conversion.nowToDateTime()) + "_" + Conversion.getHour(Conversion.nowToDateTime()) + ".csv"

  override def getOutputGoldPath() = GOLD + Conversion.getYearMonthDay(new DateTime()) + "_TGD_" + Conversion.getHour(Conversion.nowToDateTime()) + ".csv"

  override def getOutputRefineryPath() = REFINERY + Conversion.getYearMonthDay(new DateTime()) + "_TGD_" + Conversion.getHour(Conversion.nowToDateTime()) + ".csv"

  override def Depart(): Boolean = true

  override def Arrive(): Boolean = false

  override def Panneau() : String = "TGD"
}


object TraitementTgd extends TraitementTgd