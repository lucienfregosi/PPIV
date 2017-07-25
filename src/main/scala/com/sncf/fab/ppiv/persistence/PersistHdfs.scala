package com.sncf.fab.ppiv.persistence

import com.sncf.fab.ppiv.business.{TgaTgdInput, TgaTgdOutput}
import com.sncf.fab.ppiv.utils.{AppConf, Conversion}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}
import org.joda.time.DateTime

/**
  * Created by Smida-Bassem on 15/05/2017.
  * Service de sauvegarde dans HDFS
  */
object PersistHdfs extends Serializable {
  /**
    *
    * @param ds sauvegarde le dataset issu des fichiers tga/tgd nettoyés
    */

  def persisteTgaTgdParsedIntoHdfs(ds: Dataset[TgaTgdInput], hdfsRefineryPath:String): Unit = {
    ds.toDF().write.format("com.databricks.spark.csv").save(hdfsRefineryPath)
  }

  /**
    * @param df le dataset issu des fichiers TGA TGD et le referentiel des gares
    */
  def persisteQualiteAffichageIntoHdfs(df: DataFrame, hdfsGoldPath:String): Unit = {


    df.write.format("com.databricks.spark.csv").save(hdfsGoldPath)
  }

  def persisteCyclesFinisHdfs (df: DataFrame, sc : SparkContext) : Unit = {

    val path= "hdfs:/data1/GARES/refinery/PPIV_PHASE2/REJET/Cyclesfinis.csv"
    df.coalesce(1).write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv").save(path)

    df.coalesce(1).write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv").save("refinery/PPIV_PHASE2/REJET/")

  }

  def persisteRejetFieldHdfs (df: DataFrame) : Unit = {

    val path= "hdfs:/data1/GARES/refinery/PPIV_PHASE2/REJET/RejectedField.csv"
    df.write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv").save(path)

  }

  def persisteRejetCycleHdfs (df: DataFrame) : Unit = {

    val path= "hdfs:/data1/GARES/refinery/PPIV_PHASE2/REJET/RejectedCycles.csv"
    df.coalesce(1).write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv").save(path)

  }
}
