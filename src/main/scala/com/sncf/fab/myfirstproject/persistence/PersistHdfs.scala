package com.sncf.fab.myfirstproject.persistence

import com.sncf.fab.myfirstproject.business.{QualiteAffichage, TgaTgdParsed}
import com.sncf.fab.myfirstproject.utils.{AppConf, Conversion}
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

  def persisteTgaTgdParsedIntoHdfs(ds: Dataset[TgaTgdParsed],tgType:String): Unit = {
        ds.write.mode(SaveMode.Overwrite).csv(AppConf.REFINERY_HDFS)
  }

  /**
    * @param ds le dataset issu des fichiers TGA TGD et le referentiel des gares
    */
  def persisteQualiteAffichageIntoHdfs(ds: Dataset[QualiteAffichage],tgType:String): Unit = {
    ds.write.mode(SaveMode.Overwrite).csv(AppConf.GOLD+ Conversion.getYearMonthDay(new DateTime())+tgType)
  }

}
