package com.sncf.fab.ppiv.persistence
import com.sncf.fab.ppiv.business.{TgaTgdInput, TgaTgdIntermediate, TgaTgdOutput}
import com.sncf.fab.ppiv.pipelineData.TraitementTga
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext
/**
  * Created by simoh-labdoui on 11/05/2017.
  * Service de sauvegarde
  */
object PersistHive extends Serializable {
  /**
    *
    * @param ds sauvegarde le dataset issu des fichiers tga/tgd nettoyés
    */
  def persisteTgaTgdParsedHive(ds: Dataset[TgaTgdInput]): Unit = {
  }
  /**
    * @param df le dataset issu des fichiers TGA TGD et le referentiel des gares
    */
  def persisteQualiteAffichageHive(df: DataFrame, sc : SparkContext): Unit = {
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    val dfHive = hiveContext.createDataFrame(df.rdd, df.schema)
    dfHive.registerTempTable("dataToSaveToHive")
    hiveContext.sql("INSERT INTO TABLE ppiv_ref.iv_tgatgd3 select * from dataToSaveToHive")
  }

  def persisteRejectFeield(ds: Dataset[TgaTgdInput], sc : SparkContext): Unit = {
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    val dfHiveField = hiveContext.createDataFrame(ds.toDF().rdd, ds.toDF().schema)
    dfHiveField.registerTempTable("rejetField")
    hiveContext.sql("INSERT INTO TABLE ppiv_ref.iv_tgatgd_rejet_field select * from rejetField")
  }

  def persisteRejectCycle(ds: Dataset[TgaTgdIntermediate], sc : SparkContext): Unit = {
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    val dfHiveCycle = hiveContext.createDataFrame(ds.toDF().rdd, ds.toDF().schema)
    println("Nb of cycles non valides : "+ dfHiveCycle.count())
    hiveContext.sql("INSERT INTO TABLE ppiv_ref.iv_tgatgd_rejet_cycle select * from rejetCycle")

  }
}