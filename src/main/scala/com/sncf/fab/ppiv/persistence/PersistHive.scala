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
  def persisteQualiteAffichageHive(ds: Dataset[TgaTgdOutput], sc : SparkContext, sqlContext :SQLContext): Unit = {


    val hiveContext = new HiveContext(sc)

    val dfToSave = ds.toDF()

    // Sauvegarde dans Hive
    dfToSave.registerTempTable("dataToSaveHive")
    //sqlContext.sql("insert into table iv_tgatgd select * from dataToSaveHive")
    sqlContext.sql("create table testHive as select * from dataToSaveHive")
    //hiveContext.sql("show databases").show()

    //val df = sqlContext.sql("select * from dataToSaveHive")




  }

}
