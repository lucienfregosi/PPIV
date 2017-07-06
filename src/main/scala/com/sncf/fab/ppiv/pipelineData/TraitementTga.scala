package com.sncf.fab.ppiv.pipelineData

import com.sncf.fab.ppiv.utils.AppConf._
import com.sncf.fab.ppiv.utils.Conversion
import org.joda.time.DateTime

/**
  * Created by simoh-labdoui on 11/05/2017.
  */
class TraitementTga extends SourcePipeline {


  override def getSource() = LANDING_WORK  + Conversion.getYearMonthDay(Conversion.nowToDateTime()) + "/TGA-" + Conversion.getYearMonthDay(Conversion.nowToDateTime()) + "_" + Conversion.getHour(Conversion.nowToDateTime()) + ".csv"

  override def getOutputGoldPath() = GOLD + Conversion.getYearMonthDay(new DateTime()) + "_" + Conversion.getHour(Conversion.nowToDateTime()) + ".csv"

  override def getOutputRefineryPath() = REFINERY + Conversion.getYearMonthDay(new DateTime()) + "_" + Conversion.getHour(Conversion.nowToDateTime()) + ".2csv"

  override def Depart(): Boolean = false

  override def Arrive(): Boolean = true

  override def Panneau() : String = "TGA"
}

object TraitementTga extends TraitementTga
