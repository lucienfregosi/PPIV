package com.sncf.fab.ppiv.persistence

import com.sncf.fab.ppiv.business.{TgaTgdInput, TgaTgdOutput}
import com.sncf.fab.ppiv.utils.{AppConf, Conversion}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}
import org.joda.time.DateTime

/**
  * Created by Smida-Bassem on 15/05/2017.
  * Service de sauvegarde
  */
object PersistLocal extends Serializable {
  /**
    *
    * @param ds sauvegarde le dataset issu des fichiers tga/tgd nettoyés
    */

  def persisteTgaTgdParsedIntoFs(ds: Dataset[TgaTgdInput], tgType:String): Unit = {

        ds.toDF().write.format("com.databricks.spark.csv").save(AppConf.REFINERY + Conversion.getYearMonthDay(new DateTime()) + tgType)

  }

  /**
    * @param df le dataset issu des fichiers TGA TGD et le referentiel des gares
    */
  def persisteQualiteAffichageIntoFs(df: DataFrame, tgType:String): Unit = {


    df.write.format("com.databricks.spark.csv").save(AppConf.GOLD + Conversion.getYearMonthDay(new DateTime()) + tgType)

  }

}
