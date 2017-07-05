package com.sncf.fab.ppiv.utils

import java.io.File

import com.sncf.fab.ppiv.business.TgaTgdInput
import com.sncf.fab.ppiv.pipelineData.TraitementTga
import com.sncf.fab.ppiv.utils.AppConf._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.LongType
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by ELFI03951 on 30/06/2017.
  */
class validateCycle extends Specification{

  def is = s2"""

This is a specification fot the "validateCycle" output
The 'validateCycle'  output   should
  Cycle without voie shoud be false                                     $e1
  Cycle with at least one voie shoud be true                            $e2
  Maj after the departure date  retard  10 shoud be false               $e3
  Maj at least one before departure plus retard plus 10 should be true  $e4
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

  val pathSansVoie = new File("src/test/resources/data/trajet_sans_voie.csv").getAbsolutePath
  val pathAvecVoie = new File("src/test/resources/data/trajet_avec_voie.csv").getAbsolutePath()
  val pathAvecEventApres = new File("src/test/resources/data/event_apres_depart.csv").getAbsolutePath()
  val pathAvecEventAvant = new File("src/test/resources/data/event_avant_depart.csv").getAbsolutePath()

  val dsSansVoie = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "false")
    .option("delimiter", ",")
    .load(pathSansVoie).toDF(header: _*)
    .withColumn("maj", 'maj.cast(LongType))
    .withColumn("heure", 'heure.cast(LongType))
    .as[TgaTgdInput];


  val dsAvecVoie = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "false")
    .option("delimiter", ",")
    .load(pathAvecVoie).toDF(header: _*)
    .withColumn("maj", 'maj.cast(LongType))
    .withColumn("heure", 'heure.cast(LongType))
    .as[TgaTgdInput];


  val dsAvecEventApres = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "false")
    .option("delimiter", ",")
    .load(pathAvecEventApres).toDF(header: _*)
    .withColumn("maj", 'maj.cast(LongType))
    .withColumn("heure", 'heure.cast(LongType))
    .as[TgaTgdInput];

  val dsAvecEventAvant = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "false")
    .option("delimiter", ",")
    .load(pathAvecEventAvant).toDF(header: _*)
    .withColumn("maj", 'maj.cast(LongType))
    .withColumn("heure", 'heure.cast(LongType))
    .as[TgaTgdInput];




  def e1 = sourcePipeline.validateCycle(dsSansVoie, sqlContext) must beFalse
  def e2 = sourcePipeline.validateCycle(dsAvecVoie, sqlContext) must beTrue
  def e3 = sourcePipeline.validateCycle(dsAvecEventApres, sqlContext) must beFalse
  def e4 = sourcePipeline.validateCycle(dsAvecEventAvant, sqlContext) must beTrue


}
