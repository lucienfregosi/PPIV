package com.sncf.fab.ppiv.pipelineData.libPipeline

import com.sncf.fab.ppiv.business.{TgaTgdInput, TgaTgdIntermediate}
import com.sncf.fab.ppiv.persistence.Persist
import org.apache.spark.SparkContext
import org.apache.spark.sql.Dataset
import org.joda.time.DateTime

/**
  * Created by ELFI03951 on 12/07/2017.
  */
object Reject {
  def saveFieldRejected(dsFieldRejected: Dataset[TgaTgdInput], sparkContext: SparkContext, timeToProcess: DateTime): Unit ={

    Persist.save(dsFieldRejected.toDF() , "RejetField", sparkContext,timeToProcess)
  }

  def saveCycleRejected(dsFieldRejected: Dataset[TgaTgdIntermediate], sparkContext: SparkContext,timeToProcess: DateTime): Unit ={

    Persist.save(dsFieldRejected.toDF() , "RejetCycle", sparkContext,timeToProcess)
  }

}
