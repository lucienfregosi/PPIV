package com.sncf.fab.ppiv.test.buildCycleOver

import java.io.File

import com.sncf.fab.ppiv.business.{TgaTgdCycleId, TgaTgdInput}
import com.sncf.fab.ppiv.pipelineData.libPipeline.BuildCycleOver
import com.sncf.fab.ppiv.utils.AppConf.{PPIV, SPARK_MASTER}
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.sql.types.LongType
import org.apache.spark.{SparkConf, SparkContext}
import org.specs2._

import scala.io.Source


/**
  * Created by ELFI03951 on 30/06/2017.
  */
class getEcentCycleIdSpec extends Specification{

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


  val path = "PPIV/src/test/resources/data/events"
  val eventdf = sqlContext.read.format("com.databricks.spark.csv").load(path).as[TgaTgdInput]


  val path2 = "PPIV/src/test/resources/data/cycles"
  val cycledf = sqlContext.read.format("com.databricks.spark.csv").load(path2).as[TgaTgdCycleId]

/*

  def readFile( file : String) = {
    for {
      line <- Source.fromFile(file).getLines().drop(1).toVector
      values = line.split(",",-1)
    } yield TgaTgdInput(values(0), values(1).toLong, values(2), values(3), values(4),values(5),values(6),values(7),values(8),values(9).toLong,values(10),values(11))
  }

  // Read CycleId File
  val dsCycleOver = Seq(("AAATGA8691081498992360","1498992360","00"), ("AVVTGA53731499077020", "1499077020", "00"))

  val newNamesTgaTgdCycle = Seq("cycle_id","heure","retard")

  val  cycleDf = sc.parallelize(dsCycleOver)
    .toDF(newNamesTgaTgdCycle: _*)
      .withColumn("heure", 'heure.cast(LongType))
    .as[TgaTgdCycleId]

// Read TgaTgd Input File
  val eventsFile = new File("src/test/resources/data/events.csv").getAbsolutePath

  val events = readFile(eventsFile ).toSeq

  val newNamesTgaTgd = Seq("gare","maj","train","ordes","num","type","picto","attribut_voie","voie","heure","etat","retard")

  val eventDf =  sc.parallelize(events)
    .toDF(newNamesTgaTgd: _*)
    .withColumn("heure", 'heure.cast(LongType))
    .as[TgaTgdInput]


  */
  //BuildCycleOver.getEventCycleId (eventDf, cycleDf, sqlContext, sc, "TGA")
  BuildCycleOver.getEventCycleId (eventdf, cycledf, sqlContext, sc, "TGA")

  def e1 ="true" must beEqualTo("true")


}