package com.sncf.fab.ppiv.persistence

import com.sncf.fab.ppiv.business.{TgaTgdOutput, TgaTgdInput}
import com.sncf.fab.ppiv.utils.{AppConf, Conversion}
import org.apache.spark.sql.{Dataset}
import org.elasticsearch.spark.rdd.EsSpark
/**
  * Created by Smida-Bassem on 15/05/2017.
  * Service de sauvegarde dans un index elastic
  */
object PersistElastic extends Serializable {
  /**
    *
    * @param ds sauvegarde le dataset issu des fichiers tga/tgd nettoyés
    */

  def persisteTgaTgdParsedIntoEs(ds: Dataset[TgaTgdInput], tgType:String): Unit = {
    EsSpark.saveToEs(ds.rdd,tgType)
  }

  /**
    * @param ds le dataset issu des fichiers TGA TGD et le referentiel des gares
    */
  def persisteQualiteAffichageIntoEs(ds: Dataset[TgaTgdOutput], tgType:String): Unit = {
    EsSpark.saveToEs(ds.rdd,tgType)
  }

}
