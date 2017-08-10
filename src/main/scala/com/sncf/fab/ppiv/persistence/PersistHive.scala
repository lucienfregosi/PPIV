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

    println("This Table will be saved in Hive")
    df.show()
    //val path= "hdfs:/data1/GARES/refinery/PPIV_PHASE2/QualiteAffichage/FichierValide.csv"
    //df.write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv").save(path)

    // To save in a single part we can add coalesce(1) to df.write

    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    val dfHive = hiveContext.createDataFrame(df.rdd, df.schema)

    dfHive.registerTempTable("dataToSaveHive")

    //hiveContext.sql("LOAD DATA INPATH '/data1/GARES/refinery/PPIV_PHASE2/QualiteAffichage/FichierValide.csv' INTO TABLE ppiv_ref.iv_tgatgd")
    hiveContext.sql("INSERT INTO TABLE ppiv_ref.iv_tgatgddemo4 select * from dataToSaveHive")

  }


  def persisteRejetHive (df: DataFrame, sc : SparkContext): Unit = {

    val pathRejet= "hdfs:/data1/GARES/refinery/PPIV_PHASE2/QualiteAffichage/FichierRejet190717.csv"
    df.coalesce(1).write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv").save(pathRejet)

    //val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
   // val dfHive = hiveContext.createDataFrame(df.rdd, df.schema)
   // dfHive.registerTempTable("dataToSaveHiveRejet")
   // hiveContext.sql("INSERT INTO TABLE ppiv_ref.iv_tgatgdtmpRejet select * from dataToSaveHiveRejet")

  }

}
