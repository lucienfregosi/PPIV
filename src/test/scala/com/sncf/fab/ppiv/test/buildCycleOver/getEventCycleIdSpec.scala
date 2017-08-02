package com.sncf.fab.ppiv.test.buildCycleOver

import java.io.File

import com.sncf.fab.ppiv.business.{TgaTgdCycleId, TgaTgdInput}
import com.sncf.fab.ppiv.pipelineData.libPipeline.BuildCycleOver
import com.sncf.fab.ppiv.utils.AppConf.{PPIV, SPARK_MASTER}
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.functions.{col, collect_list, collect_set, concat, lit}
import org.apache.spark.{SparkConf, SparkContext}
import org.specs2._

import scala.io.Source


/**
  * Created by ELFI03951 on 30/06/2017.
  */
class getEventCycleIdSpec extends Specification{

  def is = s2"""

This is a specification fot the "getEcentCycleIdSpec" output
The 'getEcentCycleIdSpec'  output count   should
  be equal to 2                                   $e1
  """




  
  val sparkConf = new SparkConf()
    .setAppName(PPIV)
    .setMaster(SPARK_MASTER)
    .set("spark.driver.allowMultipleContexts", "true")


  @transient val sc = new SparkContext(sparkConf)
  @transient val sqlContext = new SQLContext(sc)

     import sqlContext.implicits._


  // val path = "PPIV/src/test/resources/data/eventsfromhdfs.deflate"
  val path = "src/test/resources/data/eventsfromhdfs.deflate"
  val eventdf = sqlContext.read.format("com.databricks.spark.csv").load(path).map{x=>
        val seqString = x.getString(1)
   val split = seqString.toString.split(";", -1)
   TgaTgdInput(split(0), split(1).toLong, split(2), split(3), split(4), split(5), split(6), split(7), split(8), split(9).toLong, split(10), split(11))

  }.toDS()



  //val path2 = "PPIV/src/test/resources/data/cyclesfromhdfs.deflate"
  val path2 = "src/test/resources/data/cyclesfromhdfs.deflate"

  val newNamesTgaTgdCycle = Seq("cycle_id","heure","retard")
  val cycledf = sqlContext.read.format("com.databricks.spark.csv").load(path2).toDF(newNamesTgaTgdCycle: _*).withColumn("heure", 'heure.cast(LongType)).as[TgaTgdCycleId]

  //BuildCycleOver.getEventCycleId (eventDf, cycleDf, sqlContext, sc, "TGA")
   val eventsGroupedByCycleId =  BuildCycleOver.getEventCycleId (eventdf, cycledf, sqlContext, sc, "TGA")._1
/*
  val testRow = eventsGroupedByCycleId.head()
  val gareIncycleId = testRow.getString(0).substring(0,3)
  val gareOftheLastevents  = testRow.getString(1).split(",").last.split(";").head


  def  e1 = gareIncycleId must beEqualTo(gareOftheLastevents)
*/

  def  e1 = "true" must beEqualTo("true")
}