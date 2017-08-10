package com.sncf.fab.ppiv.persistence

import com.sncf.fab.ppiv.business.TgaTgdOutput
import com.sncf.fab.ppiv.pipelineData.TraitementTga
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset}
import com.sncf.fab.ppiv.utils.AppConf._
import org.joda.time.DateTime

/**
  * Created by ELFI03951 on 12/07/2017.
  */
// TODO: Améliorer ca avec un switch
object Persist {

  def save(ivTgaTgd: DataFrame, persistMethod: String, sc: SparkContext, startTimePipeline: DateTime) : Unit ={
    if (persistMethod.contains("fs"))
      PersistLocal.persisteQualiteAffichageIntoFs(ivTgaTgd, TraitementTga.getOutputRefineryPath(startTimePipeline))

    if (persistMethod.contains("hive"))
      PersistHive.persisteQualiteAffichageHive(ivTgaTgd, sc)
    if (persistMethod.contains("hdfs"))
      PersistHdfs.persisteQualiteAffichageIntoHdfs(ivTgaTgd, TraitementTga.getOutputRefineryPath(startTimePipeline))
    if (persistMethod.contains("es"))
      PersistElastic.persisteQualiteAffichageIntoEs(ivTgaTgd, OUTPUT_INDEX)
    if (persistMethod.contains("RejetField"))
      PersistHdfs.persisteRejetFieldHdfs(ivTgaTgd)
    if (persistMethod.contains("RejetCycle"))
      PersistHdfs.persisteRejetCycleHdfs(ivTgaTgd)
    if (persistMethod.contains("InputPostprocss"))
      PersistHdfs.persistBeforePostprocess(ivTgaTgd)

  }
}
