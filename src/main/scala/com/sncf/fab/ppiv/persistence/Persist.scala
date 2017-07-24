package com.sncf.fab.ppiv.persistence

import com.sncf.fab.ppiv.business.TgaTgdOutput
import com.sncf.fab.ppiv.pipelineData.TraitementTga
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset}
import com.sncf.fab.ppiv.utils.AppConf._

/**
  * Created by ELFI03951 on 12/07/2017.
  */
object Persist {
  def save(ivTgaTgd: DataFrame, persistMethod: String, sc: SparkContext) : Unit ={
    if (persistMethod.contains("fs"))
      PersistLocal.persisteQualiteAffichageIntoFs(ivTgaTgd, TraitementTga.getOutputRefineryPath())
    if (persistMethod.contains("hive"))
      PersistHive.persisteQualiteAffichageHive(ivTgaTgd, sc)
    if (persistMethod.contains("hdfs"))
      PersistHdfs.persisteQualiteAffichageIntoHdfs(ivTgaTgd, TraitementTga.getOutputRefineryPath())
    if (persistMethod.contains("es"))
      PersistElastic.persisteQualiteAffichageIntoEs(ivTgaTgd, OUTPUT_INDEX)
    if (persistMethod.contains("CyclFinis"))
      PersistHdfs.persisteCyclesFinisHdfs(ivTgaTgd)
    if (persistMethod.contains("Rejet"))
      PersistHive.persisteRejetHive(ivTgaTgd, sc)
  }
}

