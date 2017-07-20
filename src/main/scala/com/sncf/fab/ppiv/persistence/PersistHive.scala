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

    val path= "hdfs:/data1/GARES/refinery/PPIV_PHASE2/QualiteAffichage/FichierValide19/07/17.csv"
    df.coalesce(1).write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv").save(path)
   //  df.write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv").save(path)
/*
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    val dfHive = hiveContext.createDataFrame(df.rdd, df.schema)

    dfHive.registerTempTable("NewdataToSaveHive2")
    val t = hiveContext.sql("select * from NewdataToSaveHive2 limit 10")
    t.show()
    hiveContext.sql("INSERT INTO TABLE ppiv_ref.iv_tgatgdtmp6 select * from NewdataToSaveHive2")
*/
  }


  def persisteRejetHive (df: DataFrame, sc : SparkContext): Unit = {

    val pathRejet= "hdfs:/data1/GARES/refinery/PPIV_PHASE2/QualiteAffichage/FichierRejet19/07/17.csv"
    df.coalesce(1).write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv").save(pathRejet)

    //val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
   // val dfHive = hiveContext.createDataFrame(df.rdd, df.schema)
   // dfHive.registerTempTable("dataToSaveHiveRejet")
   // hiveContext.sql("INSERT INTO TABLE ppiv_ref.iv_tgatgdtmpRejet select * from dataToSaveHiveRejet")

  }

}
