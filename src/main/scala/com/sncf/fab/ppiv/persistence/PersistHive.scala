package com.sncf.fab.ppiv.persistence
import com.sncf.fab.ppiv.business.{TgaTgdInput, TgaTgdOutput}
import com.sncf.fab.ppiv.pipelineData.TraitementTga
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
  def persisteQualiteAffichageHive(ds: Dataset[TgaTgdOutput], sc : SparkContext): Unit = {

    /*
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    ds.toDF().registerTempTable("dataToSaveHive")
    hiveContext.sql("create table testHive as select * from dataToSaveHive")
    */

    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    // Sauvegarde dans HDFS
    val hdfsRefineryPath = TraitementTga.getOutputRefineryPath()

    ds.toDF().write.format("com.databricks.spark.csv").save(hdfsRefineryPath)

    // Chargement des données de HDFS dans Hive
    hiveContext.sql("CREATE EXTERNAL TABLE IF NOT EXISTS ppiv_ref.testUniqueee (nom_de_la_gare String, agence String, segmentation  String, uic String, x String, y String, id_train String, num_train String, type String, origine_destination String, type_panneau String, dateheure2 String) row format delimited fields terminated by ','")
    hiveContext.sql("LOAD DATA INPATH '/data1/GARES/refinery/PPIV_PHASE2/20170629_14.csv' INTO TABLE ppiv_ref.testUniqueee")

    hiveContext.sql("FROM src SELECT * LIMIT 10").collect().foreach(println)








  }

}
