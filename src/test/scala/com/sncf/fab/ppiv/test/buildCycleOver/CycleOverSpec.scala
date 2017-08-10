package com.sncf.fab.ppiv.test.buildCycleOver

import java.io.File

import com.sncf.fab.ppiv.business.{TgaTgdCycleId, TgaTgdInput}
import com.sncf.fab.ppiv.pipelineData.TraitementTga
import com.sncf.fab.ppiv.pipelineData.libPipeline.{BuildCycleOver, ValidateData}
import com.sncf.fab.ppiv.utils.AppConf.{PPIV, SPARK_MASTER}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.LongType
import org.specs2._

import scala.io.Source


/**
  * Created by ELFI03951 on 30/06/2017.
  */
class CycleOverSpec extends Specification{

  def is = s2"""

This is a specification fot the "CycleOverSpec" output
The  number of finished cycles should be equal to two                                 $e1
  """


  val sparkConf = new SparkConf()
    .setAppName(PPIV)
    .setMaster(SPARK_MASTER)
    .set("spark.driver.allowMultipleContexts", "true")

  @transient val sc = new SparkContext(sparkConf)
  @transient val sqlContext = new SQLContext(sc)

  import sqlContext.implicits._



  val dsCycleOver = Seq(("AMQTGA8600111500997860","15079","00"), ("AMQTGA8600111500997860","179",""))
  val newNamesTgaTgdCycle = Seq("cycle_id","heure","retard")

  val  cycleDf = sc.parallelize(dsCycleOver)
    .toDF(newNamesTgaTgdCycle: _*)
      .withColumn("heure", 'heure.cast(LongType))
    .as[TgaTgdCycleId]

 // The  number of finished cycles should be equal to two
  def e1 = BuildCycleOver.filterCycleOver(cycleDf, sqlContext).count().toString must beEqualTo("2")


}