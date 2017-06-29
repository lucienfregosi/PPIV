package com.sncf.fab.ppiv.persistence
import com.sncf.fab.ppiv.business.{TgaTgdInput, TgaTgdOutput}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, SQLContext, SaveMode}
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
    * @param ds le dataset issu des fichiers TGA TGD et le referentiel des gares
    */
  def persisteQualiteAffichageHive(ds: Dataset[TgaTgdOutput], sc : SparkContext): Unit = {


    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    ds.toDF().registerTempTable("dataToSaveHive")
    hiveContext.sql("create table testHive as select * from dataToSaveHive")







  }

}
