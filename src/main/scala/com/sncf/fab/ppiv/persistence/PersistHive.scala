package com.sncf.fab.ppiv.persistence
import com.sncf.fab.ppiv.business.{TgaTgdInput, TgaTgdOutput}
import com.sncf.fab.ppiv.pipelineData.TraitementTga
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext, SaveMode}
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
    * @param df le dataset issu des fichiers TGA TGD et le referentiel des gares
    */
  def persisteQualiteAffichageHive(df: DataFrame, sc : SparkContext): Unit = {


    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    val dfHive = hiveContext.createDataFrame(df.rdd, df.schema)
    // Sauvegarde dans HDFS
    //val hdfsRefineryPath = TraitementTga.getOutputRefineryPath()
    //ds.toDF().write.format("com.datasbricks.spark.csv").save(hdfsRefineryPath)


    dfHive.registerTempTable("dataToSaveHive")
    hiveContext.sql("INSERT INTO TABLE ppiv_ref.iv_tgatgdOK select * from dataToSaveHive")

    //hiveContext.sql("CREATE EXTERNAL TABLE IF NOT EXISTS ppiv_ref.iv_tgatgd9 as select * from dataToSaveHive")

    // Load data to HDFS
    //hiveContext.sql("LOAD DATA INPATH '" + hdfsRefineryPath.replaceAll("hdfs:","") + "' INTO TABLE ppiv_ref.iv_tgatgd3")

    // Affichage pour vérifier que cela a bien marché
    //println("log pour être sur que ca a bien marché")
    //hiveContext.sql("FROM ppiv_ref.iv_tgatgd3 SELECT * LIMIT 10").collect().foreach(println)

    // Problème de out of bounds exception créer la structure finale ca marchera mieux

  }

}
