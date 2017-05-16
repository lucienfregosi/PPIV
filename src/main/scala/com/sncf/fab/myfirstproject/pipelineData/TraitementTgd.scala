package com.sncf.fab.myfirstproject.pipelineData

import com.sncf.fab.myfirstproject.utils.AppConf._
import com.sncf.fab.myfirstproject.utils.Conversion
import org.joda.time.DateTime

/**
  * Created by simoh-labdoui on 11/05/2017.
  */
class TraitementTgd extends SourcePipeline {

  override def getSource()=LANDING_WORK+ Conversion.getYearMonthDay(new DateTime())+TGD

  override def getOutputGoldPath()=GOLD+ Conversion.getYearMonthDay(new DateTime())+"_TGD"

  override def getOutputRefineryPath()=REFINERY+ Conversion.getYearMonthDay(new DateTime())+"_TGD"

  override def Depart(): Boolean = true

  override def Arrive(): Boolean = false
}


object TraitementTgd extends TraitementTgd