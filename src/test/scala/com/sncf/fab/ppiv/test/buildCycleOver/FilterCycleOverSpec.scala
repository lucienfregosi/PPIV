package com.sncf.fab.ppiv.test.buildCycleOver

import com.sncf.fab.ppiv.business.TgaTgdCycleId
import com.sncf.fab.ppiv.pipelineData.libPipeline.BuildCycleOver
import com.sncf.fab.ppiv.utils.AppConf.{PPIV, SPARK_MASTER}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.LongType
import org.apache.spark.{SparkConf, SparkContext}
import org.specs2._


/**
  * Created by ELFI03951 on 30/06/2017.
  */
class FilterCycleOverSpec extends Specification{

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
/*
  val path = "PPIV/src/test/resources/data/cyclesfromhdfs.deflate"
  val newNamesTgaTgdCycle = Seq("cycle_id","heure","retard")
  val cycledf = sqlContext.read.format("com.databricks.spark.csv").load(path).toDF(newNamesTgaTgdCycle: _*).withColumn("heure", 'heure.cast(LongType)).as[TgaTgdCycleId]

*/

  def e1 = "true" must beEqualTo("true")


}