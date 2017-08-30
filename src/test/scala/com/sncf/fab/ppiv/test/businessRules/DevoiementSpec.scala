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

'getNbTotaldevoiement' for 2devoiement.csv output be a equal to  2                   $e17

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
  val path2DevoiFile = new File("src/test/resources/data/businessRules/2devoiements.csv").getAbsolutePath

  //Load File
  val dsDevoiSpec = readFile(pathDevoiFile).toSeq
  val ds2DevoiSpec = readFile(path2DevoiFile).toSeq

  //devoiementInfo

  val devInfo2 = BusinessRules.allDevoimentInfo(ds2DevoiSpec)
  val devInfo = BusinessRules.allDevoimentInfo(dsDevoiSpec)

  val nbTotalDev2 = BusinessConversion.getNbTotaldevoiement(BusinessRules.getTypeDevoiement(ds2DevoiSpec, devInfo2,1), BusinessRules.getTypeDevoiement(ds2DevoiSpec, devInfo2,2), BusinessRules.getTypeDevoiement(ds2DevoiSpec, devInfo2,3), BusinessRules.getTypeDevoiement(ds2DevoiSpec, devInfo2,4))

  def e17 = nbTotalDev2 must beEqualTo(3)

  def e1 = BusinessRules.getTypeDevoiement(dsDevoiSpec,devInfo, 1).split("-")(2).toString.replaceAll("\\s", "") must beEqualTo("Non_Affiche")

  def e2 = BusinessRules.getTypeDevoiement(dsDevoiSpec,devInfo, 1).split("-")(0).toString.replaceAll("\\s", "") must beEqualTo("A")

  def e3 = BusinessRules.getTypeDevoiement(dsDevoiSpec,devInfo, 1).split("-")(1).toString.replaceAll("\\s", "") must beEqualTo("B")

  def e4 = BusinessRules.getTypeDevoiement(dsDevoiSpec,devInfo, 2).split("-")(2).toString.replaceAll("\\s", "") must beEqualTo("Affiche")

  def e5 = BusinessRules.getTypeDevoiement(dsDevoiSpec,devInfo, 2).split("-")(0).toString.replaceAll("\\s", "") must beEqualTo("B")

  def e6 = BusinessRules.getTypeDevoiement(dsDevoiSpec,devInfo, 2).split("-")(1).toString.replaceAll("\\s", "") must beEqualTo("4")


  def e7 = BusinessRules.getTypeDevoiement(dsDevoiSpec,devInfo, 3).split("-")(2).toString.replaceAll("\\s", "") must beEqualTo("Affiche")

  def e8 = BusinessRules.getTypeDevoiement(dsDevoiSpec,devInfo, 3).split("-")(0).toString.replaceAll("\\s", "") must beEqualTo("4")

  def e9 = BusinessRules.getTypeDevoiement(dsDevoiSpec,devInfo, 3).split("-")(1).toString.replaceAll("\\s", "") must beEqualTo("R")

  def e10 = BusinessRules.getTypeDevoiement(dsDevoiSpec,devInfo, 4).split("-")(2).toString.replaceAll("\\s", "") must beEqualTo("Affiche")

  def e11 = BusinessRules.getTypeDevoiement(dsDevoiSpec,devInfo, 4).split("-")(0).toString.replaceAll("\\s", "") must beEqualTo("R")

  def e12 = BusinessRules.getTypeDevoiement(dsDevoiSpec,devInfo, 4).split("-")(1).toString.replaceAll("\\s", "") must beEqualTo("N")



  //tester le Nb Total de dev
  val nbTotalDev = BusinessConversion.getNbTotaldevoiement(BusinessRules.getTypeDevoiement(dsDevoiSpec, devInfo,1), BusinessRules.getTypeDevoiement(dsDevoiSpec, devInfo,2), BusinessRules.getTypeDevoiement(dsDevoiSpec, devInfo,3), BusinessRules.getTypeDevoiement(dsDevoiSpec, devInfo,4))
  val nbTotalDevAffiche = BusinessConversion.getNbDevoiement_affiche(BusinessRules.getTypeDevoiement(dsDevoiSpec, devInfo,1), BusinessRules.getTypeDevoiement(dsDevoiSpec, devInfo,2), BusinessRules.getTypeDevoiement(dsDevoiSpec, devInfo,3), BusinessRules.getTypeDevoiement(dsDevoiSpec,devInfo,4))
  val nbTotalDevNonAffiche = BusinessConversion.getNbDevoiement_non_affiche(BusinessRules.getTypeDevoiement(dsDevoiSpec, devInfo,1), BusinessRules.getTypeDevoiement(dsDevoiSpec, devInfo,2), BusinessRules.getTypeDevoiement(dsDevoiSpec, devInfo,3), BusinessRules.getTypeDevoiement(dsDevoiSpec,devInfo,4))
  val caracDevoiement = BusinessConversion.getCaracDevoiement(BusinessRules.getTypeDevoiement(dsDevoiSpec, devInfo,1), BusinessRules.getTypeDevoiement(dsDevoiSpec, devInfo,2), BusinessRules.getTypeDevoiement(dsDevoiSpec, devInfo,3), BusinessRules.getTypeDevoiement(dsDevoiSpec,devInfo,4))


  def e13 = nbTotalDev must beEqualTo(4)

  def e14 = nbTotalDevAffiche must beEqualTo(3)

  def e15 = nbTotalDevNonAffiche must beEqualTo(1)

  def e16 =caracDevoiement must beEqualTo("Devoiement affiche")



}