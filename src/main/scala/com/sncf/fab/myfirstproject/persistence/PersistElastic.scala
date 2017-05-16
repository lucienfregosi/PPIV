package com.sncf.fab.myfirstproject.persistence

import com.sncf.fab.myfirstproject.business.{QualiteAffichage, TgaTgdParsed}
import com.sncf.fab.myfirstproject.utils.{AppConf, Conversion}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.joda.time.DateTime
import org.elasticsearch.spark.sql.EsSparkSQL
/**
  * Created by Smida-Bassem on 15/05/2017.
  * Service de sauvegarde dans un index elastic
  */
object PersistElastic extends Serializable {
  /**
    *
    * @param ds sauvegarde le dataset issu des fichiers tga/tgd nettoyés
    */

  def persisteTgaTgdParsedIntoEs(ds: Dataset[TgaTgdParsed], tgType:String): Unit = {
    EsSparkSQL.saveToEs(ds,"spark/docs")
  }

  /**
    * @param ds le dataset issu des fichiers TGA TGD et le referentiel des gares
    */
  def persisteQualiteAffichageIntoEs(ds: Dataset[QualiteAffichage],tgType:String): Unit = {
    EsSparkSQL.saveToEs(ds,"qualite/docs")
  }

}
