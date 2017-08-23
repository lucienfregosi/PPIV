package com.sncf.fab.ppiv.test.businessRules

import java.io.File

import com.sncf.fab.ppiv.business.TgaTgdInput
import com.sncf.fab.ppiv.pipelineData.libPipeline.{BusinessConversion, BusinessRules}
import org.specs2.Specification

import scala.io.Source

/**
  * Created by ELFI03951 on 04/07/2017.
  */
class DevoiementSpec extends Specification {


  def is =
    s2"""
This is a specification for the "getTypeDevoiement" output
'getTypeDevoiement' output should be equal to  'Non Affiche'                         $e1
'getTypeDevoiement' output should be equal to  'A'                                   $e2
'getTypeDevoiement' output should be equal to  'B'                                   $e3

'getTypeDevoiement2' output be a equal to  'Affiche'                                 $e4
'getTypeDevoiement2' output be a equal to  'B'                                       $e5
'getTypeDevoiement2' output be a equal to  '4'                                       $e6

'getTypeDevoiement3' output be a equal to  'Affiche'                                 $e7
'getTypeDevoiement3' output be a equal to  '4'                                       $e8
'getTypeDevoiement3' output be a equal to  'R'                                       $e9

'getTypeDevoiement4' output be a equal to  'Affiche'                                 $e10
'getTypeDevoiement4' output be a equal to  'R'                                       $e11
'getTypeDevoiement4' output be a equal to  'N'                                       $e12

'getNbTotaldevoiement' output be a equal to  4                                       $e13
'getNbTotaldevoiement' output be a equal to  3                                       $e14
'getNbTotaldevoiement' output be a equal to  1                                       $e15
'getNbTotaldevoiement' output be a equal to  Devoiement affiche                      $e16

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


  def e1 = BusinessRules.getTypeDevoiement(dsDevoiSpec).split("-")(2).toString.replaceAll("\\s", "") must beEqualTo("Non_Affiche")

  def e2 = BusinessRules.getTypeDevoiement(dsDevoiSpec).split("-")(0).toString.replaceAll("\\s", "") must beEqualTo("A")

  def e3 = BusinessRules.getTypeDevoiement(dsDevoiSpec).split("-")(1).toString.replaceAll("\\s", "") must beEqualTo("B")

  def e4 = BusinessRules.getTypeDevoiement2(dsDevoiSpec).split("-")(2).toString.replaceAll("\\s", "") must beEqualTo("Affiche")

  def e5 = BusinessRules.getTypeDevoiement2(dsDevoiSpec).split("-")(0).toString.replaceAll("\\s", "") must beEqualTo("B")

  def e6 = BusinessRules.getTypeDevoiement2(dsDevoiSpec).split("-")(1).toString.replaceAll("\\s", "") must beEqualTo("4")


  def e7 = BusinessRules.getTypeDevoiement3(dsDevoiSpec).split("-")(2).toString.replaceAll("\\s", "") must beEqualTo("Affiche")

  def e8 = BusinessRules.getTypeDevoiement3(dsDevoiSpec).split("-")(0).toString.replaceAll("\\s", "") must beEqualTo("4")

  def e9 = BusinessRules.getTypeDevoiement3(dsDevoiSpec).split("-")(1).toString.replaceAll("\\s", "") must beEqualTo("R")

  def e10 = BusinessRules.getTypeDevoiement4(dsDevoiSpec).split("-")(2).toString.replaceAll("\\s", "") must beEqualTo("Affiche")

  def e11 = BusinessRules.getTypeDevoiement4(dsDevoiSpec).split("-")(0).toString.replaceAll("\\s", "") must beEqualTo("R")

  def e12 = BusinessRules.getTypeDevoiement4(dsDevoiSpec).split("-")(1).toString.replaceAll("\\s", "") must beEqualTo("N")


  //devoiementInfo
  val devInfo = BusinessRules.allDevoimentInfo(dsDevoiSpec)
  //tester le Nb Total de dev
  val nbTotalDev = BusinessConversion.getNbTotaldevoiement(BusinessRules.getTypeDevoiement(dsDevoiSpec), BusinessRules.getTypeDevoiement2(dsDevoiSpec), BusinessRules.getTypeDevoiement3(dsDevoiSpec), BusinessRules.getTypeDevoiement4(dsDevoiSpec))
  val nbTotalDevAffiche = BusinessConversion.getNbDevoiement_affiche(BusinessRules.getTypeDevoiement(dsDevoiSpec), BusinessRules.getTypeDevoiement2(dsDevoiSpec), BusinessRules.getTypeDevoiement3(dsDevoiSpec), BusinessRules.getTypeDevoiement4(dsDevoiSpec))
  val nbTotalDevNonAffiche = BusinessConversion.getNvDevoiement_non_affiche(BusinessRules.getTypeDevoiement(dsDevoiSpec), BusinessRules.getTypeDevoiement2(dsDevoiSpec), BusinessRules.getTypeDevoiement3(dsDevoiSpec), BusinessRules.getTypeDevoiement4(dsDevoiSpec))
  val caracDevoiement = BusinessConversion.getCracDevoiement(BusinessRules.getTypeDevoiement(dsDevoiSpec), BusinessRules.getTypeDevoiement2(dsDevoiSpec), BusinessRules.getTypeDevoiement3(dsDevoiSpec), BusinessRules.getTypeDevoiement4(dsDevoiSpec))


  def e13 = nbTotalDev must beEqualTo(4)

  def e14 = nbTotalDevAffiche must beEqualTo(3)

  def e15 = nbTotalDevNonAffiche must beEqualTo(1)

  def e16 =caracDevoiement must beEqualTo("Devoiement affiche")
}