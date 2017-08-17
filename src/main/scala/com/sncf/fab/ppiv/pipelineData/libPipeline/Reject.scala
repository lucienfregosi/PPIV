package com.sncf.fab.ppiv.pipelineData.libPipeline

import com.sncf.fab.ppiv.business.{TgaTgdInput, TgaTgdIntermediate}
import com.sncf.fab.ppiv.persistence.Persist
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, SaveMode}
import org.joda.time.DateTime
import com.sncf.fab.ppiv.utils.AppConf.{REJECTED_FIELD,REJECTED_CYCLE}

/**
  * Created by ELFI03951 on 12/07/2017.
  */
object Reject {

  // Sauvegarde des objets TgaTgd rejetés de la validation champ a champ dans HDFS
  def saveFieldRejected(dsFieldRejected: Dataset[TgaTgdInput], sparkContext: SparkContext, timeToProcess: DateTime): Unit ={

    // Sauvegarde des rejets avec les champs invalides
    dsFieldRejected.toDF().write.mode(SaveMode.Append).format("com.databricks.spark.csv").option("header", "true").save(REJECTED_FIELD)


  }

  // Sauvegarde des cycles n'ayant pas passé la validation des cycles
  def saveCycleRejected(dsFieldRejected: Dataset[TgaTgdIntermediate], sparkContext: SparkContext,timeToProcess: DateTime): Unit ={

    // Sauvegarde des cycles rejetés dans HDFS
    dsFieldRejected.toDF().write.mode(SaveMode.Append).format("com.databricks.spark.csv").option("header", "true").save(REJECTED_CYCLE)

  }

}
