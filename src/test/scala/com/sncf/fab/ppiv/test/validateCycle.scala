package com.sncf.fab.ppiv.test
import java.io.File

import com.sncf.fab.ppiv.business.TgaTgdInput
import com.sncf.fab.ppiv.pipelineData.TraitementTga
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{DoubleType, LongType}
import com.sncf.fab.ppiv.utils.AppConf._
import org.apache.spark.sql.{Dataset, SQLContext}
import org.joda.time.{DateTime, DateTimeZone}
import org.specs2._
import scala.io.Source



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

  def readFile( file : String) = {
    for {
      line <- Source.fromFile(file).getLines().drop(1).toVector
      values = line.split(",").map(_.trim)
    } yield TgaTgdInput(values(0), values(1).toLong, values(2), values(3), values(4),values(5),values(6),values(7),values(8),values(9).toLong,values(10),values(11))
  }

  val sourcePipeline = new TraitementTga



  val pathSansVoie = new File("src/test/resources/data/trajet_sans_voie.csv").getAbsolutePath
  val pathAvecVoie = new File("src/test/resources/data/trajet_avec_voie.csv").getAbsolutePath()
  val pathAvecEventApres = new File("src/test/resources/data/event_apres_depart.csv").getAbsolutePath()
  val pathAvecEventAvant = new File("src/test/resources/data/event_avant_depart.csv").getAbsolutePath()





  val dsSansVoie = readFile(pathSansVoie).toSeq
  val dsAvecVoie = readFile(pathAvecVoie).toSeq
  val dsAvecEventApres = readFile(pathAvecEventApres).toSeq
  val dsAvecEventAvant = readFile(pathAvecEventAvant).toSeq



  def e1 = sourcePipeline.validateCycle(dsSansVoie) must beFalse
  def e2 = sourcePipeline.validateCycle(dsAvecVoie) must beTrue
  def e3 = sourcePipeline.validateCycle(dsAvecEventApres) must beFalse
  def e4 = sourcePipeline.validateCycle(dsAvecEventAvant) must beTrue


}