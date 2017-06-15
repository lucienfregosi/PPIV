package com.sncf.fab.ppiv.pipelineData

import com.sncf.fab.ppiv.utils.AppConf._
import com.sncf.fab.ppiv.utils.Conversion
import org.joda.time.DateTime

/**
  * Created by simoh-labdoui on 11/05/2017.
  */
class TraitementTgd extends SourcePipeline {

  override def getSource()=LANDING_WORK+ Conversion.getYearMonthDay(new DateTime())+TGD

  override def getOutputGoldPath()=GOLD_HDFS + Conversion.getYearMonthDay(new DateTime())+"_TGD"

  override def getOutputRefineryPath()= REFINERY_HDFS + Conversion.getYearMonthDay(new DateTime())+"_TGD"

  override def Depart(): Boolean = true

  override def Arrive(): Boolean = false
}


object TraitementTgd extends TraitementTgd