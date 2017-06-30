package com.sncf.fab.ppiv.test
import com.sncf.fab.ppiv.business.TgaTgdInput
import com.sncf.fab.ppiv.pipelineData.TraitementTga
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{DoubleType, LongType}
import com.sncf.fab.ppiv.utils.AppConf._
import org.apache.spark.sql.{Dataset, SQLContext}
import org.joda.time.{DateTime, DateTimeZone}
import org.specs2._



/**
  * Created by ELFI03951 on 30/06/2017.
  */
class validateApplyStickingPlaster extends Specification{ def is = s2"""

This is a specification fot the "validateApplyStickingPlaster" output
The 'validateApplyStickingPlaster'  output   should
  be a Dataset[TgaTgdInput]                                                           $e1
  Field maj should be the previous day than field hour if maj > 18 and heure < 12     $e2
  Field maj should be the same day than field hour if maj < 18 or heure > 12          $e3
  """

  val sparkConf = new SparkConf()
    .setAppName(PPIV)
    .setMaster(SPARK_MASTER)
    .set("es.nodes", HOST)
    .set("es.port", "9201")
    .set("es.index.auto.create", "true")

  @transient val sc = new SparkContext(sparkConf)
  @transient val sqlContext = new SQLContext(sc)

  val sourcePipeline = new TraitementTga

  import sqlContext.implicits._

  val newNamesTgaTgd = Seq("gare","maj","train","ordes","num","type","picto","attribut_voie","voie","heure","etat","retard")
  val dsToSuccess = sc.parallelize(Seq(("ABC", "1498828411", "20", "DEST", "123", "TER", "12345", "I", "A", "1498839248", "IND", "05"))).toDF(newNamesTgaTgd: _*).withColumn("maj", 'maj.cast(LongType)).withColumn("heure", 'heure.cast(LongType)).as[TgaTgdInput]
  val dsToFail = sc.parallelize(Seq(("ABC", "1498839248", "20", "DEST", "123", "TER", "12345", "I", "A", "1498828542", "IND", "05"))).toDF(newNamesTgaTgd: _*).withColumn("maj", 'maj.cast(LongType)).withColumn("heure", 'heure.cast(LongType)).as[TgaTgdInput]


  def e1 = sourcePipeline.applyStickingPlaster(dsToFail, sqlContext) must haveClass[Dataset[TgaTgdInput]]

  def e2 = new DateTime(sourcePipeline.applyStickingPlaster(dsToFail, sqlContext).toDF().head().getLong(1)).getDayOfMonth must be_== (new DateTime(sourcePipeline.applyStickingPlaster(dsToFail, sqlContext).toDF().head().getLong(9)).getDayOfMonth - 1).when((new DateTime(sourcePipeline.applyStickingPlaster(dsToFail, sqlContext).toDF().head().getLong(1)).getHourOfDay) > 18 && (new DateTime(sourcePipeline.applyStickingPlaster(dsToFail, sqlContext).toDF().head().getLong(9)).getHourOfDay) < 12 )

  def e3 = new DateTime(sourcePipeline.applyStickingPlaster(dsToFail, sqlContext).toDF().head().getLong(1)).getDayOfMonth must be_== (new DateTime(sourcePipeline.applyStickingPlaster(dsToFail, sqlContext).toDF().head().getLong(9)).getDayOfMonth).when((new DateTime(sourcePipeline.applyStickingPlaster(dsToFail, sqlContext).toDF().head().getLong(1)).getHourOfDay) < 18 && (new DateTime(sourcePipeline.applyStickingPlaster(dsToFail, sqlContext).toDF().head().getLong(9)).getHourOfDay) > 12 )

  sc.stop()
}
