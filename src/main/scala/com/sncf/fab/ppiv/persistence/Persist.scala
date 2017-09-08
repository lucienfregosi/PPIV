package com.sncf.fab.ppiv.persistence
import com.sncf.fab.ppiv.business.TgaTgdOutput
import com.sncf.fab.ppiv.pipelineData.TraitementTga
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset}
import com.sncf.fab.ppiv.utils.AppConf._
import org.apache.spark.sql.hive.HiveContext
import org.joda.time.DateTime
/**
  * Created by ELFI03951 on 12/07/2017.
  */
// TODO: Améliorer ca avec un switch
object Persist {
  def save(ivTgaTgd: DataFrame, persistMethod: String, sc: SparkContext, startTimePipeline: DateTime, hiveContext: HiveContext) : Unit ={
    // Persistance dans Hive
    if (persistMethod.contains("hive"))
      PersistHive.persisteQualiteAffichageHive(ivTgaTgd, sc, hiveContext)
    // Persistance dans HDFS
    else if (persistMethod.contains("hdfs"))
      PersistHdfs.persisteQualiteAffichageIntoHdfs(ivTgaTgd, TraitementTga.getOutputRefineryPath(startTimePipeline))
    // Persistance dans elasticsearch
    else if (persistMethod.contains("es"))
      PersistElastic.persisteQualiteAffichageIntoEs(ivTgaTgd, OUTPUT_INDEX)
    // Persistance dasn le file system
    else if (persistMethod.contains("fs"))
      PersistLocal.persisteQualiteAffichageIntoFs(ivTgaTgd, TraitementTga.getOutputRefineryPath(startTimePipeline))
  }
}