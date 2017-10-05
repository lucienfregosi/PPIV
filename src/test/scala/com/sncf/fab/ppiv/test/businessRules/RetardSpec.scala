package com.sncf.fab.ppiv.test.businessRules

import java.io.File

import com.sncf.fab.ppiv.business.TgaTgdInput
import com.sncf.fab.ppiv.pipelineData.libPipeline.BusinessRules
import org.specs2.Specification
import com.sncf.fab.ppiv.utils.Conversion

import scala.io.Source

/**
  * Created by ELFI03951 on 04/07/2017.
  */
class RetardSpec extends Specification {


  def is =
    s2"""
This is a specification for 'getDernierRetardAnnonce' and ' getAffichageRetard'  output
  'getDernierRetardAnnonce' output should be  equal to  0                                          $e0
  'getDernierRetardAnnonce' output should be  equal to  7 * 60 seconds                             $e1
  'getAffichageRetard" should  be a equal to 1499077209                                            $e2
  'getAffichageRetard" for recette should  be a equal to 1504459514                                $e3
  'getAffichageRetard" for complexe recette should  be a equal to 1504459514                       $e4
  'getDernierRetardAnnonce" for complexe recette should  be a equal to 300sec                      $e5
  'getAffichageRetard" for recette 05102017  should  be a equal to 1506521940                      $e6
  'getAffichageRetard" for train number 2868  should  be a equal to 1506511752                     $e6
  'getAffichageRetard" for train number should  be a equal to 0                                    $e7
  'getAffichageRetard" for train number should  be a equal to 0                                    $e8
  """


  def readFile(file: String) = {
    for {
      line <- Source.fromFile(file).getLines()
      values = line.split(",", -1)
    } yield TgaTgdInput(values(0), values(1).toLong, values(2), values(3), values(4), values(5), values(6), values(7), values(8), values(9).toLong, values(10), values(11))
  }


  //val pathRetardFile = new File("PPIV/src/test/resources/data/ExampleOfEventsForRetardTest.csv").getAbsolutePath
  val pathRetardFile = new File("src/test/resources/data/businessRules/ExampleOfEventsForRetardTest.csv").getAbsolutePath
  val pathRetardFile2 = new File("src/test/resources/data/businessRules/retard.csv").getAbsolutePath
  val pathRetardFile3 = new File("src/test/resources/data/businessRules/retardComplexe.csv").getAbsolutePath
  val pathRetardFile4 = new File("src/test/resources/data/businessRules/retardRecette05102017.csv").getAbsolutePath
  val pathRetardFile5 = new File("src/test/resources/data/businessRules/retard2868.csv").getAbsolutePath
  val pathRetardFile6 = new File("src/test/resources/data/businessRules/RetardRecette2.csv").getAbsolutePath



  val dsRetardSpec = readFile(pathRetardFile).toSeq
  val dsRetardSpec2 = readFile(pathRetardFile2).toSeq
  val dsRetardSpec3 = readFile(pathRetardFile3).toSeq
  val dsRetardSpec4 = readFile(pathRetardFile4).toSeq
  val dsRetardSpec5 = readFile(pathRetardFile5).toSeq
  val dsRetardSpec6 = readFile(pathRetardFile6).toSeq


  val Retard = (7 * 60).toString


  def e0 = BusinessRules.getDernierRetardAnnonce(dsRetardSpec6).toString must beEqualTo("0")
 // def e1 = BusinessRules.getDernierRetardAnnonce(dsRetardSpec).toString must beEqualTo(Retard)
  def e1 = BusinessRules.getDernierRetardAnnonce(dsRetardSpec).toString must beEqualTo("0")
  def e2 = BusinessRules.getAffichageRetard(dsRetardSpec).toString must beEqualTo("0")
  def e3 = BusinessRules.getAffichageRetard(dsRetardSpec2).toString must beEqualTo("1504459514")
  def e4 = BusinessRules.getAffichageRetard(dsRetardSpec3).toString must beEqualTo("1504460856")
  def e5 = BusinessRules.getDernierRetardAnnonce(dsRetardSpec3).toString must beEqualTo("300")
  def e6 = BusinessRules.getAffichageRetard(dsRetardSpec4).toString must beEqualTo("1506521940")
  def e7 = BusinessRules.getAffichageRetard(dsRetardSpec5).toString must beEqualTo("1506511752")
  def e8 = BusinessRules.getAffichageRetard(dsRetardSpec6).toString must beEqualTo("0")



}