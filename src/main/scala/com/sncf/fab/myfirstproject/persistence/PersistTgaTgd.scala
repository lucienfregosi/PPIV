package com.sncf.fab.myfirstproject.persistence
import com.sncf.fab.myfirstproject.business.TgaTgd
import org.apache.spark.sql.Dataset

/**
  * Created by simoh-labdoui on 11/05/2017.
  */
object PersistTgaTgd extends Serializable {
  /**
    *
    * @param ds sauvegarde le dataset issu des fichiers tga/tgd nettoyés
    */

  def persisteTgaTgdCleandHive(ds: Dataset[TgaTgd]): Unit = {

  }

  /**
    * @param ds le dataset issu des fichiers TGA TGD et le referentiel des gares
    */
  def persisteTgaTgdGoldHive(ds: Dataset[TgaTgd]): Unit = {

  }

}
