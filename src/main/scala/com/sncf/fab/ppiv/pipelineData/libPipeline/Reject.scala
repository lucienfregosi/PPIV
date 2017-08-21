package com.sncf.fab.ppiv.pipelineData.libPipeline

import com.sncf.fab.ppiv.business.{TgaTgdInput, TgaTgdIntermediate}
import com.sncf.fab.ppiv.persistence.Persist
import org.apache.spark.SparkContext
import org.apache.spark.sql.Dataset
import org.joda.time.DateTime
import com.sncf.fab.ppiv.utils.AppConf._

/**
  * Created by ELFI03951 on 12/07/2017.
  */
object Reject {
  def saveFieldRejected(dsFieldRejected: Dataset[TgaTgdInput], sparkContext: SparkContext, timeToProcess: DateTime, panneau: String): Unit ={

    // Sauvegarde des rejets de champs dans refinery

    dsFieldRejected.toDF().coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save(REJECTED_FIELD + "/AMATH" + panneau + "_"  + timeToProcess.getMillis + ".csv")
  }

  def saveCycleRejected(dsFieldRejected: Dataset[TgaTgdIntermediate], sparkContext: SparkContext,timeToProcess: DateTime, panneau: String): Unit ={

    // Sauvegarde des rejets de cycles dans refinery
    dsFieldRejected.toDF().coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save(REJECTED_CYCLE + "/AMATH" + panneau + "_" + timeToProcess.getMillis + ".csv")
  }

}
