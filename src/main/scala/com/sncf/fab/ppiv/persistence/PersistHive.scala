package com.sncf.fab.ppiv.persistence
import com.sncf.fab.ppiv.business.{TgaTgdInput, TgaTgdOutput}
import com.sncf.fab.ppiv.spark.batch.TraitementPPIVDriver.LOGGER
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
  def persisteQualiteAffichageHive(ds: Dataset[TgaTgdOutput], sqlContext : SQLContext): Unit = {
    import sqlContext.implicits._

    LOGGER.info("Sauvegarde dans Hive")

    ds.toDF().write.mode(SaveMode.Append).saveAsTable("ppiv_ref.iv_tgatgd")

  }

}
