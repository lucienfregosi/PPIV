package com.sncf.fab.ppiv.test

import java.io.File

import com.sncf.fab.ppiv.business.TgaTgdInput
import com.sncf.fab.ppiv.pipelineData.TraitementTga
import com.sncf.fab.ppiv.utils.AppConf.{PPIV, SPARK_MASTER}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.LongType
import org.specs2.Specification

/**
  * Created by ELFI03951 on 04/07/2017.
  */
class validateDelaiAffichage extends Specification{


  def is = s2"""
This is a specification for the "getAffichage" output
The 'getAffichageDuree1'  output   should
  be a positive number beginning from 0                                        $e1
  With trajet_sans_retard.csv the result should be 13                          $e2
  With trajet_avec_retard.csv data with delay the result should be 13          $e3
The 'getAffichageDuree2'  output   should
  be a positive number beginning from 0                                        $e4
  With trajet_sans_retard.csv the result should be 13                          $e5
  With trajet_avec_retard.csv with delay the result should be 18               $e6
  """



  val sparkConf = new SparkConf()
    .setAppName(PPIV)
    .setMaster(SPARK_MASTER)
    .set("spark.driver.allowMultipleContexts", "true")

  @transient val sc = new SparkContext(sparkConf)
  @transient val sqlContext = new SQLContext(sc)
  val sourcePipeline = new TraitementTga
  import sqlContext.implicits._

  val header = Seq("gare","maj","train","ordes","num","type","picto","attribut_voie","voie","heure","etat","retard")
  val pathSansRetard = new File("src/test/resources/data/trajet_sans_retard.csv").getAbsolutePath
  val pathAvecRetard = new File("src/test/resources/data/trajet_avec_voie.csv").getAbsolutePath()

  val dsSansRetard = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "false")
    .option("delimiter", ",")
    .load(pathSansRetard).toDF(header: _*)
    .withColumn("maj", 'maj.cast(LongType))
    .withColumn("heure", 'heure.cast(LongType))
    .as[TgaTgdInput];

  val dsAvecRetard = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "false")
    .option("delimiter", ",")
    .load(pathAvecRetard).toDF(header: _*)
    .withColumn("maj", 'maj.cast(LongType))
    .withColumn("heure", 'heure.cast(LongType))
    .as[TgaTgdInput];


  def e1 = sourcePipeline.getAffichageDuree1(dsSansRetard, sqlContext) must be_>= (0)
  def e2 = sourcePipeline.getAffichageDuree1(dsSansRetard, sqlContext) mustEqual 13
  def e3 = sourcePipeline.getAffichageDuree1(dsAvecRetard, sqlContext) mustEqual 13

  def e4 = sourcePipeline.getAffichageDuree2(dsSansRetard, sqlContext) must be_>= (0)
  def e5 = sourcePipeline.getAffichageDuree1(dsSansRetard, sqlContext) mustEqual 13
  def e6 = sourcePipeline.getAffichageDuree1(dsAvecRetard, sqlContext) mustEqual 18

}
