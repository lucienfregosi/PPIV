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
The 'CycleOver'  output count   should
  be equal to 2                                   $e1
  """


  val sparkConf = new SparkConf()
    .setAppName(PPIV)
    .setMaster(SPARK_MASTER)
    .set("spark.driver.allowMultipleContexts", "true")


  @transient val sc = new SparkContext(sparkConf)
  @transient val sqlContext = new SQLContext(sc)

  import sqlContext.implicits._

  def readFile( file : String) = {
    for {
      line <- Source.fromFile(file).getLines().drop(1).toVector
      values = line.split(",",-1)
    } yield TgaTgdCycleId(values(0), values(1).toLong, values(2))
  }

  val pathCycleOver = new File("src/test/resources/data/cycles.csv").getAbsolutePath
  val dsCycleOver = readFile(pathCycleOver).toSeq

  val newNamesTgaTgdCycle = Seq("cycle_id","heure","retard")

  val  cycleDf = sc.parallelize(dsCycleOver)
    .toDF(newNamesTgaTgdCycle: _*)
      .withColumn("heure", 'heure.cast(LongType))
    .as[TgaTgdCycleId]



  def e1 = BuildCycleOver.filterCycleOver(cycleDf, sqlContext).count().toString must beEqualTo("2")


}