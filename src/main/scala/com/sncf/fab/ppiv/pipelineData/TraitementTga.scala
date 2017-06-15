package com.sncf.fab.ppiv.pipelineData

import com.sncf.fab.ppiv.utils.AppConf._
import com.sncf.fab.ppiv.utils.Conversion
import org.joda.time.DateTime

/**
  * Created by simoh-labdoui on 11/05/2017.
  */
class TraitementTga extends SourcePipeline {


  override def getSource() = LANDING_WORK_HDFS  + "TGA-" + Conversion.getYearMonthDay(Conversion.nowToDateTime()) + "_" + Conversion.getHour(Conversion.nowToDateTime())

  override def getOutputGoldPath() = GOLD_HDFS + Conversion.getYearMonthDay(new DateTime()) + "_TGA"

  override def getOutputRefineryPath() = REFINERY_HDFS + Conversion.getYearMonthDay(new DateTime()) + "_TGA"

  override def Depart(): Boolean = false

  override def Arrive(): Boolean = true
}

object TraitementTga extends TraitementTga