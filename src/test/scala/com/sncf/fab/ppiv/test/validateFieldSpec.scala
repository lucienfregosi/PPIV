package com.sncf.fab.ppiv.test
import com.sncf.fab.ppiv.business.TgaTgdInput
import com.sncf.fab.ppiv.parser.DatasetsParser
import com.sncf.fab.ppiv.pipelineData.TraitementTga
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{DoubleType, LongType}
import com.sncf.fab.ppiv.utils.AppConf._
import org.apache.spark.sql.SQLContext
import org.joda.time.{DateTime, DateTimeZone}
import org.specs2._

/**
  * Created by ESGI10601 on 27/06/2017.
  */
class validateFieldSpec extends Specification{
  sequential
  def is = s2"""

This is a specification fot the "validateField" output
The 'validateField'  output   should
Gare has three capital letters                                           $e1
Maj is less or equal to Current Timestamp                                $e2
Train field is in [1:20]                                                 $e3
Ordes field has only capital letters                                     $e4
Num_train is a number                                                    $e5
Num_train is a postive number                                            $e6
Type in capital letters                                                  $e7
Attribut_voie is I or empty                                              $e9
Voie is one character                                                    $e10
Etat should be in the list {SUP, IND, ARR}                               $e12
Retard should be 2 or 3 digits                                           $e13

  """

  val sparkConf = new SparkConf()
    .setAppName(PPIV)
    .setMaster(SPARK_MASTER)
    .set("spark.driver.allowMultipleContexts", "true")


  @transient val sc = new SparkContext(sparkConf)
  @transient val sqlContext = new SQLContext(sc)
  val sourcePipeline = new TraitementTga
  import sqlContext.implicits._

  val newNamesTgaTgd = Seq("gare","maj","train","ordes","num","type","picto","attribut_voie","voie","heure","etat","retard")
  val testrddDf = sc.parallelize(Seq(("ABT", "15", "20", "DEST O", "123", "TER", "12345", "I", "A", "12962", "IND", "")))
    .toDF(newNamesTgaTgd: _*)
    .withColumn("maj", 'maj.cast(LongType))
    .withColumn("heure", 'heure.cast(LongType))
    .as[TgaTgdInput]

  val testrddDs  =  testrddDf.toDF().map(row => DatasetsParser.parseTgaTgdDataset(row)).toDS()

  val currentTimestamp = DateTime.now(DateTimeZone.UTC).getMillis() / 1000


  def e1 = sourcePipeline.validateField(testrddDs,sqlContext).toDF().head().getString(0) must =~("^[A-Z]{3}$")
  def e2 = sourcePipeline.validateField(testrddDs,sqlContext).toDF().head().getLong(1) must be_<=(currentTimestamp)
  def e3 = sourcePipeline.validateField(testrddDs,sqlContext).toDF().head().getString(2) must =~("^[0-2]{0,1}[0-9]$")
  def e4 = sourcePipeline.validateField(testrddDs,sqlContext).toDF().head().getString(3) must =~("^[A-Z|\\s]{1,}[A-Z]{0,}$")
  def e5 = sourcePipeline.validateField(testrddDs,sqlContext).toDF().head().getString(4) must =~("^[0-9]{1,}$")
  def e6 = sourcePipeline.validateField(testrddDs,sqlContext).toDF().head().getString(4).toInt must be_>= (0)
  def e7 = sourcePipeline.validateField(testrddDs,sqlContext).toDF().head().getString(5)  must =~ ("^[A-Z]+$")
  //def e8 = sourcePipeline.validateField(testrddDs,sqlContext).toDF().head().getString(6).toInt must be_>= (0)
  def e9 = sourcePipeline.validateField(testrddDs,sqlContext).toDF().head().getString(7) must =~("I{0,1}$")
  def e10 = sourcePipeline.validateField(testrddDs,sqlContext).toDF().head().getString(8) must =~("^[A-Z|1-9]{1}$")
  //def e11 = sourcePipeline.validateField(testrddDs,sqlContext).toDF().head().getLong(9) must be_<=(currentTimestamp)
  def e12 = sourcePipeline.validateField(testrddDs,sqlContext).toDF().head().getString(10) must beOneOf("IND", "SUP","ARR","")
  def e13 = sourcePipeline.validateField(testrddDs,sqlContext).toDF().head().getString(11) must =~ ("^(([0-9]{4})|([0-9]{2})|$|\\s)$")

 // "^[0-9]{2,4}$")

  sc.stop()



