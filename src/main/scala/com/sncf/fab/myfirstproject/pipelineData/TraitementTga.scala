package com.sncf.fab.myfirstproject.pipelineData

import com.sncf.fab.myfirstproject.utils.AppConf._
import com.sncf.fab.myfirstproject.utils.Conversion
import org.joda.time.DateTime

/**
  * Created by simoh-labdoui on 11/05/2017.
  */
class TraitementTga extends SourcePipeline {

  override def getSource() = LANDING_WORK + Conversion.getYearMonthDay(new DateTime()) + TGA

  override def getOutputGoldPath() = GOLD + "_TGA_" + Conversion.getYearMonthDay(new DateTime())

  override def getOutputRefineryPath() = REFINERY + "_TGA_" + Conversion.getYearMonthDay(new DateTime())

  override def Depart(): Boolean = false

  override def Arrive(): Boolean = true
}

object TraitementTga extends TraitementTga