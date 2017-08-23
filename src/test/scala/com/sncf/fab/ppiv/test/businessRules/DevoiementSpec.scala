package com.sncf.fab.ppiv.test.businessRules

import java.io.File

import com.sncf.fab.ppiv.business.TgaTgdInput
import com.sncf.fab.ppiv.pipelineData.libPipeline.BusinessRules
import org.specs2.Specification

import scala.io.Source

/**
  * Created by ELFI03951 on 04/07/2017.
  */
class DevoiementSpec extends Specification {


  def is =
    s2"""
This is a specification for the "getTypeDevoiement" output
'getTypeDevoiement' output should be equal to  'Affiche'                             $e1
'getTypeDevoiement2' output be a equal to  'Affiche'                                 $e2

  """


  def readFile(file: String) = {
    for {
      line <- Source.fromFile(file).getLines()
      values = line.split(",", -1)
    } yield TgaTgdInput(values(0), values(1).toLong, values(2), values(3), values(4), values(5), values(6), values(7), values(8), values(9).toLong, values(10), values(11))
  }

   //Path to the file
   // val pathDevoiFile = new File("PPIV/src/test/resources/data/ExampleOfEventForDevoiementTest.csv").getAbsolutePath
   val pathDevoiFile = new File("src/test/resources/data/businessRules/ExampleOfEventForDevoiementTest.csv").getAbsolutePath

  //Load File
  val dsDevoiSpec = readFile(pathDevoiFile).toSeq


  def e1 = BusinessRules.getTypeDevoiement(dsDevoiSpec).split("-")(2).toString.replaceAll("\\s", "") must beEqualTo("Affiche")

  def e2 = BusinessRules.getTypeDevoiement2(dsDevoiSpec).split("-")(2).toString.replaceAll("\\s", "") must beEqualTo("Affiche")

}