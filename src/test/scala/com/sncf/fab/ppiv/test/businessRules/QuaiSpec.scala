package com.sncf.fab.ppiv.test.businessRules

import java.io.File

import com.sncf.fab.ppiv.business.TgaTgdInput
import com.sncf.fab.ppiv.pipelineData.libPipeline.BusinessRules
import org.specs2.Specification

import scala.io.Source

/**
  * Created by ELFI03951 on 04/07/2017.
  */
class QuaiSpec extends Specification {


  def is =
    s2"""
This is a specification for the "getDernierQuaiAffiche" output

  The 'getDernierQuaiAffiche'output should be equal to C                                               $e1

  """


  def readFile(file: String) = {
    for {
      line <- Source.fromFile(file).getLines()
      values = line.split(",", -1)
    } yield TgaTgdInput(values(0), values(1).toLong, values(2), values(3), values(4), values(5), values(6), values(7), values(8), values(9).toLong, values(10), values(11))
  }


  val pathEtatFile = new File("src/test/resources/data/trajet_avec_Quai.csv").getAbsolutePath
  val dsEtatSpec = readFile(pathEtatFile).toSeq


  def e1 = BusinessRules.getDernierQuaiAffiche(dsEtatSpec).toString must beEqualTo("C")

}