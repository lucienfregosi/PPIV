package com.sncf.fab.ppiv.pipelineData.libPipeline

import com.sncf.fab.ppiv.business.{TgaTgdInput, TgaTgdIntermediate}
import com.sncf.fab.ppiv.persistence.{Persist, PersistHive, PersistHdfs}
import org.apache.spark.SparkContext
import org.apache.spark.sql.Dataset
import org.joda.time.DateTime
import com.sncf.fab.ppiv.utils.AppConf._
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by KG on 12/07/2017.
  */
object Reject {
  def saveFieldRejected(dsFieldRejected: Dataset[TgaTgdInput], pathToSave: String): Unit ={

    // Sauvegarde des rejets de champs dans HDFS
  //PersistHive.persisteRejectFeield(dsFieldRejected,  sparkContext, hiveContext)
    PersistHdfs.persisteRejectField(dsFieldRejected, pathToSave)
 }

  def saveCycleRejected(dsFieldRejected: Dataset[TgaTgdIntermediate], pathToSave: String): Unit ={

    // Sauvegarde des rejets de cycle dans hive
    //PersistHive.persisteRejectCycle(dsFieldRejected,  sparkContext, hiveContext)
    PersistHdfs.persisteRejectCycle(dsFieldRejected, pathToSave)
  }

}
