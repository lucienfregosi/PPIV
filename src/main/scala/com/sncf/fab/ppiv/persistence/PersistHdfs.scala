package com.sncf.fab.ppiv.persistence

import com.sncf.fab.ppiv.business.{QualiteAffichage, TgaTgdParsed}
import com.sncf.fab.ppiv.utils.{AppConf, Conversion}
import org.apache.spark.sql.{Dataset, SaveMode}
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

  def persisteTgaTgdParsedIntoHdfs(ds: Dataset[TgaTgdParsed], hdfsRefineryPath:String): Unit = {
    ds.toDF().write.format("com.databricks.spark.csv").save(hdfsRefineryPath)
  }

  /**
    * @param ds le dataset issu des fichiers TGA TGD et le referentiel des gares
    */
  def persisteQualiteAffichageIntoHdfs(ds: Dataset[QualiteAffichage], hdfsGoldPath:String): Unit = {

    println(hdfsGoldPath)
    ds.toDF().write.format("com.databricks.spark.csv").save(hdfsGoldPath)
  }

}
