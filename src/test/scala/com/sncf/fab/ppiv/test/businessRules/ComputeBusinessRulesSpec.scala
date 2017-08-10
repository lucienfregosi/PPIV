package com.sncf.fab.ppiv.test.businessRules

import java.io.File

import com.sncf.fab.ppiv.business.{TgaTgdCycleId, TgaTgdInput}
import com.sncf.fab.ppiv.pipelineData.TraitementTga
import com.sncf.fab.ppiv.pipelineData.libPipeline.BusinessRules
import com.sncf.fab.ppiv.utils.AppConf.{PPIV, SPARK_MASTER}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.LongType
import org.specs2.Specification

import scala.io.Source

/**
  * Created by ELFI03951 on 04/07/2017.
  */
class ComputeBusinessRulesSpec extends Specification {


  def is =
    s2"""
This is a specification for the "ComputeBusinessRules"
a
 Describe the output                           $e1
   """


  val sparkConf = new SparkConf()
    .setAppName(PPIV)
    .setMaster(SPARK_MASTER)
    .set("spark.driver.allowMultipleContexts", "true")


  @transient val sc = new SparkContext(sparkConf)
  @transient val sqlContext = new SQLContext(sc)


  def readFile(file: String) = {
    for {
      line <- Source.fromFile(file).getLines()
      values = line.split(",", -1)
    } yield TgaTgdInput(values(0), values(1).toLong, values(2), values(3), values(4), values(5), values(6), values(7), values(8), values(9).toLong, values(10), values(11))
  }

  // Path to the input File  (Add PPIv for local run )
  //val path = "PPIV/src/test/resources/data/Pre-ComputeBusinissRule.csv"
  val path = "src/test/resources/data/Pre-ComputeBusinissRule.csv"

  //Load File
  val EventsGroupedByCycleId = sqlContext.read.format("com.databricks.spark.csv").load(path)


  val tgatgdIntermediate = BusinessRules.computeBusinessRules(EventsGroupedByCycleId)


  def e1 = "true" must beEqualTo("true")

}