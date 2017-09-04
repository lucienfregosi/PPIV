package com.sncf.fab.ppiv.pipelineData.libPipeline

import com.sncf.fab.ppiv.business.{TgaTgdInput, TgaTgdIntermediate}
import com.sncf.fab.ppiv.persistence.{Persist, PersistHive}
import org.apache.spark.SparkContext
import org.apache.spark.sql.Dataset
import org.joda.time.DateTime
import com.sncf.fab.ppiv.utils.AppConf._

/**
  * Created by KG on 12/07/2017.
  */
object Reject {
  def saveFieldRejected(dsFieldRejected: Dataset[TgaTgdInput], sparkContext: SparkContext, timeToProcess: DateTime, panneau: String): Unit ={

    // Sauvegarde des rejets de champs dans hive
  PersistHive.persisteRejectFeield(dsFieldRejected,  sparkContext)
 }

  def saveCycleRejected(dsFieldRejected: Dataset[TgaTgdIntermediate], sparkContext: SparkContext,timeToProcess: DateTime, panneau: String): Unit ={

    // Sauvegarde des rejets de cycle dans hive
    PersistHive.persisteRejectCycle(dsFieldRejected,  sparkContext)
  }

}
