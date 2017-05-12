package com.sncf.fab.myfirstproject.persistence
import com.sncf.fab.myfirstproject.business.{QualiteAffichage, TgaTgdParsed}
import org.apache.spark.sql.Dataset

/**
  * Created by simoh-labdoui on 11/05/2017.
  * Service de sauvegarde
  */
object PersistHive extends Serializable {
  /**
    *
    * @param ds sauvegarde le dataset issu des fichiers tga/tgd nettoyés
    */

  def persisteTgaTgdParsedHive(ds: Dataset[TgaTgdParsed]): Unit = {

  }

  /**
    * @param ds le dataset issu des fichiers TGA TGD et le referentiel des gares
    */
  def persisteQualiteAffichageHive(ds: Dataset[QualiteAffichage]): Unit = {

  }

}
