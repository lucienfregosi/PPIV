package com.sncf.fab.ppiv.pipelineData.libPipeline

import com.sncf.fab.ppiv.business.{ReferentielGare, TgaTgdInput, TgaTgdOutput, TgaTgdWithoutRef}
import com.sncf.fab.ppiv.utils.Conversion
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext}

/**
  * Created by ELFI03951 on 12/07/2017.
  */
object Postprocess {
  def saveCleanData(dsToSave: Dataset[TgaTgdInput], sc: SparkContext) : Unit = {
    null
  }

  def joinReferentiel(dsTgaTgd: Dataset[TgaTgdWithoutRef],  refGares : Dataset[ReferentielGare], sqlContext : SQLContext): DataFrame = {
    // Jointure entre nos données de sorties et le référentiel
    val joinedData = dsTgaTgd.toDF().join(refGares.toDF(), dsTgaTgd.toDF().col("gare") === refGares.toDF().col("TVS"))

    joinedData
  }

  def formatTgaTgdOuput(dfTgaTgd: DataFrame, sqlContext : SQLContext, panneau: String) : Dataset[TgaTgdOutput] = {
    import sqlContext.implicits._


    val affichageFinal =  dfTgaTgd.map(row => TgaTgdOutput(
      row.getString(11),
      row.getString(22),
      row.getString(15),
      row.getString(13),
      row.getString(25),
      row.getString(26),
      row.getString(0),
      row.getString(3),
      row.getString(4),
      row.getString(2),
      panneau,
      Conversion.unixTimestampToDateTime(row.getLong(5)).toString,
      Conversion.unixTimestampToDateTime(row.getLong(7)).toString,
      Conversion.unixTimestampToDateTime(row.getLong(8)).toString,
      Conversion.unixTimestampToDateTime(row.getLong(9)).toString
    ))

    affichageFinal.toDS().as[TgaTgdOutput]

  }
}
