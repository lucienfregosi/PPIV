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
  'getDernierRetardAnnonce' output should be  equal to  7 * 60 seconds                             $e1
  'getAffichageRetard" should  be a equal to 1499077209                                            $e2
  """


  def readFile(file: String) = {
    for {
      line <- Source.fromFile(file).getLines()
      values = line.split(",", -1)
    } yield TgaTgdInput(values(0), values(1).toLong, values(2), values(3), values(4), values(5), values(6), values(7), values(8), values(9).toLong, values(10), values(11))
  }


  //val pathRetardFile = new File("PPIV/src/test/resources/data/ExampleOfEventsForRetardTest.csv").getAbsolutePath
  val pathRetardFile = new File("src/test/resources/data/ExampleOfEventsForRetardTest.csv").getAbsolutePath
  val dsRetardSpec = readFile(pathRetardFile).toSeq


  val Retard = (7 * 60).toString

  def e1 = BusinessRules.getDernierRetardAnnonce(dsRetardSpec).toString must beEqualTo(Retard)

  def e2 = BusinessRules.getAffichageRetard(dsRetardSpec).toString must beEqualTo("1499077209")

}