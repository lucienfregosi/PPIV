package com.sncf.fab.ppiv.pipelineData.libPipeline

import com.sncf.fab.ppiv.business.{TgaTgdInput, TgaTgdWithoutRef}
import org.apache.spark.SparkContext
import org.apache.spark.sql.Dataset

/**
  * Created by ELFI03951 on 12/07/2017.
  */
object Reject {
  def saveFieldRejected(dsFieldRejected: Dataset[TgaTgdInput], sparkContext: SparkContext): Unit ={

    null
  }

  def saveCycleRejected(dsFieldRejected: Dataset[TgaTgdWithoutRef], sparkContext: SparkContext): Unit ={

    null
  }

}
